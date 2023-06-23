//! The MapReduce coordinator.
//!

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ::log::{log, Level};
use anyhow::Result;
use prost::encoding::string;
use tokio::sync::Mutex;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};

use crate::rpc::coordinator::*;
use crate::*;

pub mod args;

#[derive(Debug, Clone)]
pub struct Task {
    job_id: u32,
    task_id: u32,
    worker: u32,
    file: String,

    complete: bool,
    in_progress: bool,
}

#[derive(Debug, Clone)]
pub struct Job {
    job_id: u32,
    done: bool,
    failed: bool,
    errors: Vec<String>,

    app: String,
    args: Vec<u8>,
    n_map: u32,
    n_reduce: u32,

    //control
    map_tasks: Vec<Task>,
    reduce_tasks: Vec<Task>,
}

pub struct CoordinatorState {
    worker_heartbeats: HashMap<u32, Duration>,
    job_count: u32,
    worker_count: u32,
    job_queue: Vec<u32>,
    jobs: HashMap<u32, Job>,
}

pub struct Coordinator {
    state: Arc<Mutex<CoordinatorState>>,
}

impl Coordinator {
    pub async fn new() -> Self {
        // let worker_heartbeats = HashMap::new();
        // let jobs = HashMap::new();

        let state = Arc::new(Mutex::new(CoordinatorState {
            worker_heartbeats: HashMap::new(),
            job_count: 0,
            worker_count: 0,
            job_queue: Vec::new(),
            jobs: HashMap::new(),
        }));

        // SystemTime::now()
        //     .duration_since(UNIX_EPOCH)
        //     .unwrap();

        // tokio::spawn(async move {
        //     loop {
        //         tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        //         let state = Arc::clone(&state);
        //     }
        // });

        Self {
            state: Arc::clone(&state),
        }
    }
}

fn new_job(
    job_id: u32,
    files: Vec<String>,
    output_dir: String,
    app: String,
    n_reduce: u32,
    args: Vec<u8>,
) -> Job {
    let mut map_tasks = Vec::new();
    let mut reduce_tasks = Vec::new();

    for i in 0..n_reduce {
        reduce_tasks.push(Task {
            job_id: job_id,
            task_id: i,
            worker: 0,

            file: output_dir.clone(),

            complete: false,
            in_progress: false,
        });
    }

    for (i, filename) in files.iter().enumerate() {
        map_tasks.push(Task {
            job_id: job_id,
            task_id: i as u32,
            worker: 0,
            file: filename.clone(),

            complete: false,
            in_progress: false,
        });
    }

    Job {
        job_id: job_id,
        done: false,
        failed: false,
        errors: Vec::new(),

        app: app.clone(),
        args: args.clone(),
        n_map: files.len() as u32,
        n_reduce: n_reduce,

        map_tasks: map_tasks,
        reduce_tasks: reduce_tasks,
    }
}

fn get_map_assignments(job: &Job) -> Vec<MapTaskAssignment> {
    let mut map_tasks = Vec::new();

    for task in job.map_tasks.iter() {
        assert!(task.complete);

        map_tasks.push(MapTaskAssignment {
            task: task.task_id,
            worker_id: task.worker.clone(),
        });
    }

    map_tasks
}

fn update_state_on_failed_worker(state: &mut CoordinatorState, worker_id: u32) {
    for (_, job) in state.jobs.iter_mut() {
        for task in job.map_tasks.iter_mut() {
            if task.worker == worker_id {
                task.complete = false;
                task.in_progress = false;
            }
        }

        for task in job.reduce_tasks.iter_mut() {
            if task.worker == worker_id && !task.complete {
                task.complete = false;
                task.in_progress = false;
            }
        }
    }
}

#[tonic::async_trait]
impl coordinator_server::Coordinator for Coordinator {
    async fn submit_job(
        &self,
        req: Request<SubmitJobRequest>,
    ) -> Result<Response<SubmitJobReply>, Status> {
        let mut state = self.state.lock().await;
        let req = req.get_ref();

        match crate::app::named(&req.app) {
            Ok(_) => {}
            Err(e) => {
                return Err(Status::new(Code::InvalidArgument, e.to_string()));
            }
        }

        let job = new_job(
            state.job_count,
            req.files.clone(),
            req.output_dir.clone(),
            req.app.clone(),
            req.n_reduce,
            req.args.clone(),
        );
        let job_id = job.job_id;

        state.job_count += 1;
        state.job_queue.push(job_id);
        state.jobs.insert(job_id, job);

        log!(Level::Info, "submit job: {}, {:?}", job_id, state.jobs);

        Ok(Response::new(SubmitJobReply { job_id: job_id }))
    }

    async fn poll_job(
        &self,
        req: Request<PollJobRequest>,
    ) -> Result<Response<PollJobReply>, Status> {
        let state = self.state.lock().await;
        let req = req.get_ref();

        let job = match state.jobs.get(&req.job_id) {
            Some(job) => job,
            None => {
                return Err(Status::new(Code::NotFound, "job id is invalid"));
            }
        };

        return Ok(Response::new(PollJobReply {
            done: job.done,
            failed: job.failed,
            errors: job.errors.clone(),
        }));
    }

    async fn heartbeat(
        &self,
        req: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatReply>, Status> {
        let mut state = self.state.lock().await;
        state.worker_heartbeats.insert(
            req.get_ref().worker_id,
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap(),
        );
        Ok(Response::new(HeartbeatReply {}))
    }

    async fn register(
        &self,
        _req: Request<RegisterRequest>,
    ) -> Result<Response<RegisterReply>, Status> {
        log!(Level::Info, "Register");
        let mut state = self.state.lock().await;
        let worker_id = state.worker_count;
        state.worker_heartbeats.insert(
            worker_id,
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap(),
        );
        state.worker_count += 1;
        log!(
            Level::Info,
            "Register: heartbeats: {:?}\n",
            state.worker_heartbeats
        );

        Ok(Response::new(RegisterReply {
            worker_id: worker_id,
        }))
    }

    async fn get_task(
        &self,
        req: Request<GetTaskRequest>,
    ) -> Result<Response<GetTaskReply>, Status> {
        let mut state = self.state.lock().await;
        let req = req.get_ref();

        // cycle through the workers to remove
        let mut workers_to_remove = Vec::new();

        for (worker_id, last_heartbeat) in state.worker_heartbeats.iter() {
            if SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                - last_heartbeat.as_secs()
                > TASK_TIMEOUT_SECS
            {
                workers_to_remove.push(*worker_id);
            }
        }

        for id in workers_to_remove {
            state.worker_heartbeats.remove(&id);
            log!(Level::Info, "worker dead: {}", id);
            // log!(Level::Info, "Updating state: {:?}", state.jobs);
            update_state_on_failed_worker(&mut state, id);
            // log!(Level::Info, "Updated state: {:?}", state.jobs);
        }

        // cycle through the jobs
        for i in 0..state.job_queue.len() {
            let job_id = state.job_queue.get(i as usize).unwrap().clone();
            let job = state.jobs.get_mut(&job_id).unwrap();

            let mut map_tasks_in_progress = false;
            // cycle through the map tasks
            for task in job.map_tasks.iter_mut() {
                if task.complete {
                    continue;
                }

                if task.in_progress {
                    map_tasks_in_progress = true;
                    continue;
                }

                task.worker = req.worker_id;
                task.in_progress = true;
                return Ok(Response::new(GetTaskReply {
                    job_id: job_id,
                    output_dir: "".to_string(),
                    app: job.app.clone(),
                    task: task.task_id,
                    file: task.file.clone(),
                    n_reduce: job.reduce_tasks.len() as u32,
                    n_map: job.map_tasks.len() as u32,
                    reduce: false,
                    wait: false,
                    map_task_assignments: Vec::new(),
                    args: job.args.clone(),
                }));
            }

            if map_tasks_in_progress == false {
                for task in job.reduce_tasks.iter_mut() {
                    if task.complete {
                        continue;
                    }

                    if task.in_progress {
                        continue;
                    }

                    task.worker = req.worker_id;
                    task.in_progress = true;
                    return Ok(Response::new(GetTaskReply {
                        job_id: job_id,
                        output_dir: task.file.clone(),
                        app: job.app.clone(),
                        task: task.task_id,
                        file: "".to_string(),
                        n_reduce: job.reduce_tasks.len() as u32,
                        n_map: job.map_tasks.len() as u32,
                        reduce: true,
                        wait: false,
                        map_task_assignments: get_map_assignments(&job),
                        args: job.args.clone(),
                    }));
                }
            }
        }

        Ok(Response::new(GetTaskReply {
            job_id: 0,
            output_dir: "".to_string(),
            app: "".to_string(),
            task: 0,
            file: "".to_string(),
            n_reduce: 0,
            n_map: 0,
            reduce: false,
            wait: true,
            map_task_assignments: Vec::new(),
            args: Vec::new(),
        }))
    }

    async fn finish_task(
        &self,
        req: Request<FinishTaskRequest>,
    ) -> Result<Response<FinishTaskReply>, Status> {
        let mut state = self.state.lock().await;
        let req = req.get_ref();

        let job = match state.jobs.get_mut(&req.job_id) {
            Some(job) => job,
            None => {
                return Err(Status::new(Code::NotFound, "job id is invalid"));
            }
        };

        if req.reduce {
            let task = match job.reduce_tasks.get_mut(req.task as usize) {
                Some(task) => task,
                None => {
                    return Err(Status::new(Code::NotFound, "reduce task id is invalid"));
                }
            };

            task.complete = true;
        } else {
            let task = match job.map_tasks.get_mut(req.task as usize) {
                Some(task) => task,
                None => {
                    return Err(Status::new(Code::NotFound, "map task id is invalid"));
                }
            };

            task.complete = true;
        }

        //loop through all the tasks and see if they are all complete
        let mut all_complete = true;
        for task in job.map_tasks.iter() {
            if task.complete == false {
                all_complete = false;
                break;
            }
        }
        for task in job.reduce_tasks.iter() {
            if task.complete == false {
                all_complete = false;
                break;
            }
        }

        if all_complete {
            job.done = true;

            //find and remove the job from the queue
            let mut i = 0;
            for job_id in state.job_queue.iter() {
                if job_id == &req.job_id {
                    break;
                }
                i += 1;
            }
            log!(
                Level::Info,
                "JOB COMPLETE: {}, {:?}",
                req.job_id,
                state.job_queue
            );
            state.job_queue.remove(i);
        }

        Ok(Response::new(FinishTaskReply {}))
    }

    async fn fail_task(
        &self,
        req: Request<FailTaskRequest>,
    ) -> Result<Response<FailTaskReply>, Status> {
        let mut state = self.state.lock().await;
        let req = req.get_ref();

        let job = match state.jobs.get_mut(&req.job_id) {
            Some(job) => job,
            None => {
                return Err(Status::new(Code::NotFound, "job id is invalid"));
            }
        };

        if !req.retry {
            job.failed = true;
            job.errors.push(req.error.clone());

            //find and remove the job from the queue
            let mut i = 0;
            for job_id in state.job_queue.iter() {
                if job_id == &req.job_id {
                    break;
                }
                i += 1;
            }
            log!(
                Level::Info,
                "JOB FAILED: {}, {:?}",
                req.job_id,
                state.job_queue
            );
            state.job_queue.remove(i);
        } else if req.retry && req.reduce {
            let task = match job.reduce_tasks.get_mut(req.task as usize) {
                Some(task) => task,
                None => {
                    return Err(Status::new(Code::NotFound, "reduce task id is invalid"));
                }
            };

            task.in_progress = false;
            task.complete = false;
            task.worker = 0;
        }

        Ok(Response::new(FailTaskReply {}))
    }
}

pub async fn start(_args: args::Args) -> Result<()> {
    let addr = COORDINATOR_ADDR.parse().unwrap();

    let coordinator = Coordinator::new().await;
    log!(Level::Info, "New coordinator");
    let svc = coordinator_server::CoordinatorServer::new(coordinator);
    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
