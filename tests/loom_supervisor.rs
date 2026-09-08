#[test]
fn registration_and_termination_leave_no_live_entries() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        use loom::sync::{Arc, Mutex};
        use loom::thread;

        let entries = Arc::new(Mutex::new(Vec::<u32>::new()));
        let register_entries = Arc::clone(&entries);
        let terminate_entries = Arc::clone(&entries);

        let register = thread::spawn(move || {
            register_entries.lock().unwrap().push(7);
            register_entries.lock().unwrap().retain(|pid| *pid != 7);
        });
        let terminate = thread::spawn(move || {
            let drained = std::mem::take(&mut *terminate_entries.lock().unwrap());
            assert!(drained.len() <= 1);
        });

        register.join().unwrap();
        terminate.join().unwrap();
        assert!(entries.lock().unwrap().is_empty());
    });
}
