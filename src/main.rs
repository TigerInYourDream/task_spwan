use std::collections::VecDeque;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use log::{debug, error, info};
use stock_tool_box::trade_time_util::now_millis;
use tokio::runtime::Builder;
use tokio::sync::{mpsc, Mutex};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

fn main() {
    env_logger::init();
    debug!("this is a debug {}", "start");

    let order_task = TaskSpawner::new();

    // 算法
    let mut num = 0;
    loop {
        thread::sleep(Duration::from_secs(1));
        let now = now_millis();
        let task = TaskData {
            task_num: num,
            time: now,
            lag: 50,
        };
        debug!("send-->{:?}", task);
        order_task.spawn_task(TaskData {
            task_num: num,
            ..task
        });
        num += 1;
    }
}

#[derive(Debug, Clone)]
pub struct TaskData {
    task_num: i32,
    time: i64,
    lag: i64,
}

#[derive(Clone)]
pub struct TaskSpawner {
    spawn: mpsc::Sender<TaskData>,
}

impl TaskSpawner {
    pub fn new() -> TaskSpawner {
        // 创建一个消息通道用于通信
        let (send, mut recv) = mpsc::channel::<TaskData>(10000);

        let rt = Builder::new_multi_thread()
            .thread_name("action-tokio-thread")
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();

        let delay_050 = DelayTask::new(50);

        thread::spawn(move || {
            rt.block_on(async move {
                while let Some(task) = recv.recv().await {
                    let delay_050 = delay_050.clone();
                    tokio::spawn(async move {
                        if let Err(e) = delay_050.send(task) {
                            error!("send delay_050 error: {:?}", e);
                        }
                    });
                }
                // 一旦所有的发送端超出作用域被 drop 后，`.recv()` 方法会返回 None，同时 while 循环会退出，然后线程结束
            });
        });

        TaskSpawner {
            spawn: send,
        }
    }

    pub fn spawn_task(&self, task: TaskData) {
        match self.spawn.blocking_send(task) {
            Ok(()) => {}
            Err(_) => panic!("The shared runtime has shut down."),
        }
    }
}


#[derive(Clone)]
pub struct DelayTask {
    queue: Arc<Mutex<VecDeque<TaskData>>>,
}

impl DelayTask {
    pub fn new(lag: i64) -> UnboundedSender<TaskData> {
        let (sender, recv) = mpsc::unbounded_channel();

        let mut task = Self {
            queue: Arc::new(Mutex::new(VecDeque::<TaskData>::new())),
        };

        let rt1 = Builder::new_current_thread()
            .thread_name("action-tokio-thread")
            .enable_all()
            .build()
            .unwrap();

        let q_clone = task.queue.clone();
        info!("start DelayTask new -----");

        thread::spawn(move || {
            rt1.block_on(async move {
                info!("start delay_work lag -----");
                task.delay_work(recv).await;
            });
        });

        let rt2 = Builder::new_current_thread()
            .thread_name("action-tokio-thread")
            .enable_all()
            .build()
            .unwrap();


        thread::spawn(move || {
            rt2.block_on(async move {
                info!("start que_hand new -----");
                DelayTask::que_hand(q_clone, lag).await;
            });
        });

        sender
    }

    pub async fn delay_work(&mut self, mut recv: UnboundedReceiver<TaskData>) {
        info!("start delay_work lag");
        while let Some(action) = recv.recv().await {
            let mut lock = self.queue.lock().await;
            lock.push_back(action);
        }
    }

    pub async fn que_hand(q: Arc<Mutex<VecDeque<TaskData>>>, lag: i64) {
        info!("que_hand num");
        let mut delay = tokio::time::interval(Duration::from_millis(1));
        tokio::spawn(
            async move {
                info!("[que_hand] start at lag {:?}",lag);
                loop {
                    let now = now_millis();
                    let mut lock = q.lock().await;

                    if lock.len() > 0 {
                        let mut need_pop = false;
                        let front = lock.front();
                        if let Some(front) = front {
                            //info!("[que_hand] recv--< {:?}", front);
                            if now - front.time >= lag {
                                // 发送并弹出
                                need_pop = true;
                            } else {
                                delay.tick().await;
                            }
                        }

                        if need_pop {
                            // 弹出
                            let front = lock.pop_front();
                            if let Some(front) = front {
                                handlers(front);
                            }
                        }
                    } else {
                        // 防止cpu 占用100
                        delay.tick().await;
                    }
                }
            }
        ).await.unwrap();
    }
}

pub fn handlers(front: TaskData) {
    let now = now_millis();
    info!("[good] recv<-- lag {} num {}   now_millis - task.time = {}",front.lag ,front.task_num, now - front.time);
}
