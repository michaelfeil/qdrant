use std::sync::Arc;

pub struct GpuCandidatesHeap {
    pub capacity: usize,
    pub device: Arc<gpu::Device>,
}

impl GpuCandidatesHeap {
    pub fn new(device: Arc<gpu::Device>, capacity: usize) -> gpu::GpuResult<Self> {
        let ceiled_capacity = capacity.div_ceil(device.subgroup_size()) * device.subgroup_size();
        Ok(Self {
            capacity: ceiled_capacity,
            device,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BinaryHeap;

    use common::types::{PointOffsetType, ScoredPointOffset};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;
    use crate::index::hnsw_index::gpu::shader_builder::ShaderBuilder;

    #[repr(C)]
    struct TestParams {
        input_counts: u32,
    }

    #[test]
    fn test_gpu_candidates_heap() {
        let _ = env_logger::builder()
            .is_test(true)
            .filter_level(log::LevelFilter::Trace)
            .try_init();

        let capacity = 128;
        let points_count = 128;
        let groups_count = 8;
        let inputs_count = points_count;

        let mut rng = StdRng::seed_from_u64(42);
        let inputs_data: Vec<ScoredPointOffset> = (0..inputs_count * groups_count)
            .map(|i| ScoredPointOffset {
                idx: i as PointOffsetType,
                score: rng.gen_range(-1.0..1.0),
            })
            .collect();

        let debug_messenger = gpu::PanicIfErrorMessenger {};
        let instance =
            Arc::new(gpu::Instance::new("qdrant", Some(&debug_messenger), false).unwrap());
        let device = Arc::new(
            gpu::Device::new(instance.clone(), instance.vk_physical_devices[0].clone()).unwrap(),
        );

        let gpu_candidates_heap = GpuCandidatesHeap::new(device.clone(), capacity).unwrap();

        let shader = ShaderBuilder::new(device.clone())
            .with_shader_code(include_str!("shaders/tests/test_candidates_heap.comp"))
            .with_candidates_heap_capacity(gpu_candidates_heap.capacity)
            .build();

        let input_points_buffer = Arc::new(
            gpu::Buffer::new(
                device.clone(),
                gpu::BufferType::Storage,
                inputs_count * groups_count * std::mem::size_of::<ScoredPointOffset>(),
            )
            .unwrap(),
        );

        let upload_staging_buffer = Arc::new(
            gpu::Buffer::new(
                device.clone(),
                gpu::BufferType::CpuToGpu,
                inputs_count * groups_count * std::mem::size_of::<ScoredPointOffset>(),
            )
            .unwrap(),
        );
        upload_staging_buffer.upload_slice(&inputs_data, 0);

        let mut context = gpu::Context::new(device.clone());
        context.copy_gpu_buffer(
            upload_staging_buffer.clone(),
            input_points_buffer.clone(),
            0,
            0,
            input_points_buffer.size,
        );
        context.run();
        context.wait_finish();

        let test_params_buffer = Arc::new(
            gpu::Buffer::new(
                device.clone(),
                gpu::BufferType::Uniform,
                std::mem::size_of::<TestParams>(),
            )
            .unwrap(),
        );
        upload_staging_buffer.upload(
            &TestParams {
                input_counts: inputs_count as u32,
            },
            0,
        );
        context.copy_gpu_buffer(
            upload_staging_buffer,
            test_params_buffer.clone(),
            0,
            0,
            test_params_buffer.size,
        );
        context.run();
        context.wait_finish();

        let scores_output_buffer = Arc::new(
            gpu::Buffer::new(
                device.clone(),
                gpu::BufferType::Storage,
                inputs_count * groups_count * std::mem::size_of::<ScoredPointOffset>(),
            )
            .unwrap(),
        );

        let descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_uniform_buffer(0)
            .add_storage_buffer(1)
            .add_storage_buffer(2)
            .build(device.clone());

        let descriptor_set = gpu::DescriptorSet::builder(descriptor_set_layout.clone())
            .add_uniform_buffer(0, test_params_buffer.clone())
            .add_storage_buffer(1, input_points_buffer.clone())
            .add_storage_buffer(2, scores_output_buffer.clone())
            .build();

        let pipeline = gpu::Pipeline::builder()
            .add_descriptor_set_layout(0, descriptor_set_layout.clone())
            .add_shader(shader.clone())
            .build(device.clone());

        context.bind_pipeline(pipeline, &[descriptor_set.clone()]);
        context.dispatch(groups_count, 1, 1);
        context.run();
        context.wait_finish();

        let mut scores_cpu = vec![];
        for group in 0..groups_count {
            let mut heap = BinaryHeap::<ScoredPointOffset>::new();
            for i in 0..inputs_count {
                let scored_point = inputs_data[group * inputs_count + i];
                heap.push(scored_point);
            }
            while !heap.is_empty() {
                scores_cpu.push(heap.pop().unwrap());
            }
        }

        let download_staging_buffer = Arc::new(
            gpu::Buffer::new(
                device.clone(),
                gpu::BufferType::GpuToCpu,
                scores_output_buffer.size,
            )
            .unwrap(),
        );
        context.copy_gpu_buffer(
            scores_output_buffer.clone(),
            download_staging_buffer.clone(),
            0,
            0,
            scores_output_buffer.size,
        );
        context.run();
        context.wait_finish();
        let mut scores_gpu = vec![ScoredPointOffset::default(); inputs_count * groups_count];
        download_staging_buffer.download_slice(&mut scores_gpu, 0);

        assert_eq!(scores_gpu, scores_cpu);
    }
}
