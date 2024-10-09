use std::sync::Arc;

use parking_lot::{Mutex, MutexGuard};

use crate::common::operation_error::OperationResult;

pub struct DevicesMaganer {
    pub devices: Vec<Mutex<Arc<gpu::Device>>>,
    pub wait_free: bool,
}

pub struct LockedDevice<'a> {
    pub locked_device: MutexGuard<'a, Arc<gpu::Device>>,
}

impl DevicesMaganer {
    pub fn new(
        instance: Arc<gpu::Instance>,
        filter: &str,
        start_index: usize,
        count: usize,
        wait_free: bool,
    ) -> OperationResult<Self> {
        let filter = filter.to_lowercase();
        let devices = instance
            .vk_physical_devices
            .iter()
            .filter(|device| {
                let device_name = device.name.to_lowercase();
                device_name.contains(&filter)
            })
            .cloned()
            .skip(start_index)
            .take(count)
            .filter_map(|physical_device| {
                if let Some(device) = gpu::Device::new(instance.clone(), physical_device.clone()) {
                    log::info!("Initialized GPU device: {:?}", &physical_device.name);
                    Some(Mutex::new(Arc::new(device)))
                } else {
                    log::error!("Failed to create GPU device: {:?}", &physical_device.name);
                    None
                }
            })
            .collect::<Vec<_>>();
        Ok(Self { devices, wait_free })
    }

    pub fn lock_device(&self) -> Option<LockedDevice> {
        if self.devices.is_empty() {
            return None;
        }
        loop {
            // TODO(gpu): Add timeout
            for device in &self.devices {
                if let Some(guard) = device.try_lock() {
                    return Some(LockedDevice {
                        locked_device: guard,
                    });
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
    }
}
