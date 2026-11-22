use clap::Parser;
use std::process::ExitCode;

#[derive(Debug, Parser)]
#[command(version, about = "Compare Linux thread IDs with the process ID")]
struct Arguments {
    #[arg(
        long,
        default_value_t = 4,
        value_parser = clap::value_parser!(u8).range(1..=64),
        help = "Number of worker threads (1 to 64)"
    )]
    threads: u8,
}

#[cfg(any(target_os = "linux", test))]
fn validate_ids(process_id: u32, main_id: u32, workers: &[u32]) -> Result<(), String> {
    if process_id == 0 || main_id == 0 {
        return Err("process and main thread IDs must be positive".to_owned());
    }
    if main_id != process_id {
        return Err(format!(
            "main thread ID {main_id} differs from process ID {process_id}"
        ));
    }

    let mut seen = std::collections::HashSet::new();
    seen.insert(main_id);
    for (index, &id) in workers.iter().enumerate() {
        if id == 0 {
            return Err(format!("worker {index} returned an invalid thread ID"));
        }
        if !seen.insert(id) {
            return Err(format!("worker {index} has duplicate thread ID {id}"));
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
mod linux {
    use std::sync::{Arc, Barrier, mpsc};
    use std::thread::{self, JoinHandle};

    unsafe extern "C" {
        fn gettid() -> std::ffi::c_int;
    }

    fn thread_id() -> Result<u32, String> {
        let id = unsafe { gettid() };
        if id <= 0 {
            Err(format!("gettid returned invalid thread ID {id}"))
        } else {
            Ok(id as u32)
        }
    }

    struct Worker {
        barrier: Arc<Barrier>,
        id: mpsc::Receiver<Result<u32, String>>,
        handle: JoinHandle<()>,
    }

    fn finish(workers: Vec<Worker>) -> Result<(), String> {
        let mut failed = false;
        for worker in workers {
            worker.barrier.wait();
            if worker.handle.join().is_err() {
                failed = true;
            }
        }
        if failed {
            Err("a worker thread panicked".to_owned())
        } else {
            Ok(())
        }
    }

    pub fn run(count: u8) -> Result<(), String> {
        let process_id = std::process::id();
        let main_id = thread_id()?;
        let mut workers = Vec::with_capacity(usize::from(count));

        for index in 0..count {
            let barrier = Arc::new(Barrier::new(2));
            let worker_barrier = Arc::clone(&barrier);
            let (send_id, id) = mpsc::channel();
            let handle = thread::Builder::new()
                .name(format!("worker-{index}"))
                .spawn(move || {
                    let result = std::panic::catch_unwind(thread_id)
                        .unwrap_or_else(|_| Err("could not query the thread ID".to_owned()));
                    let _ = send_id.send(result);
                    worker_barrier.wait();
                });
            match handle {
                Ok(handle) => workers.push(Worker {
                    barrier,
                    id,
                    handle,
                }),
                Err(error) => {
                    let _ = finish(workers);
                    return Err(format!("could not start worker {index}: {error}"));
                }
            }
        }

        let mut ids = Vec::with_capacity(workers.len());
        let mut failure = None;
        for (index, worker) in workers.iter().enumerate() {
            match worker.id.recv() {
                Ok(Ok(id)) => ids.push(id),
                Ok(Err(error)) => {
                    failure = Some(format!("worker {index}: {error}"));
                    break;
                }
                Err(_) => {
                    failure = Some(format!("worker {index} stopped before reporting its ID"));
                    break;
                }
            }
        }

        let validation = match failure {
            Some(error) => Err(error),
            None => super::validate_ids(process_id, main_id, &ids),
        };
        let completion = finish(workers);
        validation?;
        completion?;

        println!("Process ID: {process_id}");
        println!("Main thread ID: {main_id}");
        for (index, id) in ids.iter().enumerate() {
            println!("Worker {index} ID: {id}");
        }
        Ok(())
    }
}

fn run(arguments: Arguments) -> Result<(), String> {
    #[cfg(target_os = "linux")]
    {
        linux::run(arguments.threads)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = arguments;
        Err("unsupported platform: gettid requires Linux".to_owned())
    }
}

fn main() -> ExitCode {
    match run(Arguments::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("gettid: {error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_default_and_boundary_worker_counts() {
        assert_eq!(Arguments::try_parse_from(["gettid"]).unwrap().threads, 4);
        for count in ["1", "64"] {
            assert!(Arguments::try_parse_from(["gettid", "--threads", count]).is_ok());
        }
    }

    #[test]
    fn rejects_invalid_worker_counts() {
        for count in ["0", "65", "-1", "two", "1.5"] {
            assert!(Arguments::try_parse_from(["gettid", "--threads", count]).is_err());
        }
        assert!(Arguments::try_parse_from(["gettid", "--threads"]).is_err());
    }

    #[test]
    fn validates_distinct_thread_ids() {
        assert!(validate_ids(100, 100, &[101, 102]).is_ok());
        assert!(validate_ids(100, 100, &[101, 101]).is_err());
        assert!(validate_ids(100, 100, &[100]).is_err());
        assert!(validate_ids(100, 100, &[0]).is_err());
        assert!(validate_ids(100, 101, &[102]).is_err());
        assert!(validate_ids(0, 0, &[102]).is_err());
    }
}
