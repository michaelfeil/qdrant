use std::borrow::Cow;
use std::sync::Arc;

use common::types::PointOffsetType;
use itertools::Itertools;

use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{VectorElementType, VectorElementTypeByte, VectorElementTypeHalf};
use crate::types::Distance;
use crate::vector_storage::quantized::quantized_vectors::{
    QuantizedVectorStorage, QuantizedVectors,
};
use crate::vector_storage::{DenseVectorStorage, VectorStorage, VectorStorageEnum};

pub const ELEMENTS_PER_SUBGROUP: usize = 4;
pub const UPLOAD_CHUNK_SIZE: usize = 64 * 1024 * 1024;
pub const STORAGES_COUNT: usize = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GpuVectorStorageElementType {
    Float32,
    Float16,
    Uint8,
    Binary,
    SQ,
}

pub struct GpuVectorStorage {
    pub device: Arc<gpu::Device>,
    pub vectors_buffer: Vec<Arc<gpu::Buffer>>,
    pub sq_offsets_buffer: Option<Arc<gpu::Buffer>>,
    pub descriptor_set_layout: Arc<gpu::DescriptorSetLayout>,
    pub descriptor_set: Arc<gpu::DescriptorSet>,
    pub dim: usize,
    pub count: usize,
    pub element_type: GpuVectorStorageElementType,
    pub distance: Distance,
    pub quantization: Option<GpuQuantizationParams>,
}

pub enum GpuQuantizationParams {
    Scalar(GpuScalarQuantizationParams),
}

pub struct GpuScalarQuantizationParams {
    pub multiplier: f32,
    pub diff: f32,
}

impl GpuVectorStorage {
    pub fn new(
        device: Arc<gpu::Device>,
        vector_storage: &VectorStorageEnum,
        quantized_storage: Option<&QuantizedVectors>,
        force_half_precision: bool,
    ) -> gpu::GpuResult<Self> {
        if let Some(quantized_storage) = quantized_storage {
            Self::new_from_vector_quantization(
                device,
                vector_storage,
                &quantized_storage.storage_impl,
                force_half_precision,
            )
        } else {
            Self::new_from_vector_storage(device, vector_storage, force_half_precision)
        }
    }

    fn new_from_vector_quantization(
        device: Arc<gpu::Device>,
        vector_storage: &VectorStorageEnum,
        quantized_storage: &QuantizedVectorStorage,
        force_half_precision: bool,
    ) -> gpu::GpuResult<Self> {
        match quantized_storage {
            QuantizedVectorStorage::ScalarRam(quantized_storage) => {
                Self::new_typed::<VectorElementTypeByte>(
                    device,
                    GpuVectorStorageElementType::SQ,
                    vector_storage.distance(),
                    vector_storage.total_vector_count(),
                    |id| {
                        let (_, vector) = quantized_storage.get_quantized_vector(id);
                        Cow::Borrowed(vector)
                    },
                    Some(Box::new(|id| {
                        let (offset, _) = quantized_storage.get_quantized_vector(id);
                        offset
                    })),
                    Some(GpuQuantizationParams::Scalar(GpuScalarQuantizationParams {
                        multiplier: quantized_storage.get_multiplier(),
                        diff: quantized_storage.get_diff(),
                    })),
                )
            }
            QuantizedVectorStorage::ScalarMmap(quantized_storage) => {
                Self::new_typed::<VectorElementTypeByte>(
                    device,
                    GpuVectorStorageElementType::SQ,
                    vector_storage.distance(),
                    vector_storage.total_vector_count(),
                    |id| {
                        let (_, vector) = quantized_storage.get_quantized_vector(id);
                        Cow::Borrowed(vector)
                    },
                    Some(Box::new(|id| {
                        let (offset, _) = quantized_storage.get_quantized_vector(id);
                        offset
                    })),
                    Some(GpuQuantizationParams::Scalar(GpuScalarQuantizationParams {
                        multiplier: quantized_storage.get_multiplier(),
                        diff: quantized_storage.get_diff(),
                    })),
                )
            }
            QuantizedVectorStorage::PQRam(_) => {
                log::warn!("GPU does not support product quantization, use original vector data");
                Self::new_from_vector_storage(device, vector_storage, force_half_precision)
            }
            QuantizedVectorStorage::PQMmap(_) => {
                log::warn!("GPU does not support product quantization, use original vector data");
                Self::new_from_vector_storage(device, vector_storage, force_half_precision)
            }
            QuantizedVectorStorage::BinaryRam(quantized_storage) => {
                Self::new_typed::<VectorElementTypeByte>(
                    device,
                    GpuVectorStorageElementType::Binary,
                    vector_storage.distance(),
                    vector_storage.total_vector_count(),
                    |id| Cow::Borrowed(quantized_storage.get_quantized_vector(id)),
                    None,
                    None,
                )
            }
            QuantizedVectorStorage::BinaryMmap(quantized_storage) => {
                Self::new_typed::<VectorElementTypeByte>(
                    device,
                    GpuVectorStorageElementType::Binary,
                    vector_storage.distance(),
                    vector_storage.total_vector_count(),
                    |id| Cow::Borrowed(quantized_storage.get_quantized_vector(id)),
                    None,
                    None,
                )
            }
            QuantizedVectorStorage::ScalarRamMulti(_) => Err(gpu::GpuError::NotSupported),
            QuantizedVectorStorage::ScalarMmapMulti(_) => Err(gpu::GpuError::NotSupported),
            QuantizedVectorStorage::PQRamMulti(_) => Err(gpu::GpuError::NotSupported),
            QuantizedVectorStorage::PQMmapMulti(_) => Err(gpu::GpuError::NotSupported),
            QuantizedVectorStorage::BinaryRamMulti(_) => Err(gpu::GpuError::NotSupported),
            QuantizedVectorStorage::BinaryMmapMulti(_) => Err(gpu::GpuError::NotSupported),
        }
    }

    fn new_from_vector_storage(
        device: Arc<gpu::Device>,
        vector_storage: &VectorStorageEnum,
        force_half_precision: bool,
    ) -> gpu::GpuResult<Self> {
        match vector_storage {
            VectorStorageEnum::DenseSimple(vector_storage) => {
                if force_half_precision {
                    Self::new_typed::<VectorElementTypeHalf>(
                        device,
                        GpuVectorStorageElementType::Float16,
                        vector_storage.distance(),
                        vector_storage.total_vector_count(),
                        |id| {
                            VectorElementTypeHalf::slice_from_float_cow(Cow::Borrowed(
                                vector_storage.get_dense(id),
                            ))
                        },
                        None,
                        None,
                    )
                } else {
                    Self::new_typed::<VectorElementType>(
                        device,
                        GpuVectorStorageElementType::Float32,
                        vector_storage.distance(),
                        vector_storage.total_vector_count(),
                        |id| Cow::Borrowed(vector_storage.get_dense(id)),
                        None,
                        None,
                    )
                }
            }
            VectorStorageEnum::DenseSimpleByte(vector_storage) => {
                Self::new_typed::<VectorElementTypeByte>(
                    device,
                    GpuVectorStorageElementType::Uint8,
                    vector_storage.distance(),
                    vector_storage.total_vector_count(),
                    |id| Cow::Borrowed(vector_storage.get_dense(id)),
                    None,
                    None,
                )
            }
            VectorStorageEnum::DenseSimpleHalf(vector_storage) => {
                Self::new_typed::<VectorElementTypeHalf>(
                    device,
                    GpuVectorStorageElementType::Float16,
                    vector_storage.distance(),
                    vector_storage.total_vector_count(),
                    |id| Cow::Borrowed(vector_storage.get_dense(id)),
                    None,
                    None,
                )
            }
            VectorStorageEnum::DenseMemmap(vector_storage) => {
                if force_half_precision {
                    Self::new_typed::<VectorElementTypeHalf>(
                        device,
                        GpuVectorStorageElementType::Float16,
                        vector_storage.distance(),
                        vector_storage.total_vector_count(),
                        |id| {
                            VectorElementTypeHalf::slice_from_float_cow(Cow::Borrowed(
                                vector_storage.get_dense(id),
                            ))
                        },
                        None,
                        None,
                    )
                } else {
                    Self::new_typed::<VectorElementType>(
                        device,
                        GpuVectorStorageElementType::Float32,
                        vector_storage.distance(),
                        vector_storage.total_vector_count(),
                        |id| Cow::Borrowed(vector_storage.get_dense(id)),
                        None,
                        None,
                    )
                }
            }
            VectorStorageEnum::DenseMemmapByte(vector_storage) => {
                Self::new_typed::<VectorElementTypeByte>(
                    device,
                    GpuVectorStorageElementType::Uint8,
                    vector_storage.distance(),
                    vector_storage.total_vector_count(),
                    |id| Cow::Borrowed(vector_storage.get_dense(id)),
                    None,
                    None,
                )
            }
            VectorStorageEnum::DenseMemmapHalf(vector_storage) => {
                Self::new_typed::<VectorElementTypeHalf>(
                    device,
                    GpuVectorStorageElementType::Float16,
                    vector_storage.distance(),
                    vector_storage.total_vector_count(),
                    |id| Cow::Borrowed(vector_storage.get_dense(id)),
                    None,
                    None,
                )
            }
            VectorStorageEnum::DenseAppendableMemmap(vector_storage) => {
                if force_half_precision {
                    Self::new_typed::<VectorElementTypeHalf>(
                        device,
                        GpuVectorStorageElementType::Float16,
                        vector_storage.distance(),
                        vector_storage.total_vector_count(),
                        |id| {
                            VectorElementTypeHalf::slice_from_float_cow(Cow::Borrowed(
                                vector_storage.get_dense(id),
                            ))
                        },
                        None,
                        None,
                    )
                } else {
                    Self::new_typed::<VectorElementType>(
                        device,
                        GpuVectorStorageElementType::Float32,
                        vector_storage.distance(),
                        vector_storage.total_vector_count(),
                        |id| Cow::Borrowed(vector_storage.get_dense(id)),
                        None,
                        None,
                    )
                }
            }
            VectorStorageEnum::DenseAppendableMemmapByte(vector_storage) => {
                Self::new_typed::<VectorElementTypeByte>(
                    device,
                    GpuVectorStorageElementType::Uint8,
                    vector_storage.distance(),
                    vector_storage.total_vector_count(),
                    |id| Cow::Borrowed(vector_storage.get_dense(id)),
                    None,
                    None,
                )
            }
            VectorStorageEnum::DenseAppendableMemmapHalf(vector_storage) => {
                Self::new_typed::<VectorElementTypeHalf>(
                    device,
                    GpuVectorStorageElementType::Float16,
                    vector_storage.distance(),
                    vector_storage.total_vector_count(),
                    |id| Cow::Borrowed(vector_storage.get_dense(id)),
                    None,
                    None,
                )
            }
            VectorStorageEnum::DenseAppendableInRam(vector_storage) => {
                if force_half_precision {
                    Self::new_typed::<VectorElementTypeHalf>(
                        device,
                        GpuVectorStorageElementType::Float16,
                        vector_storage.distance(),
                        vector_storage.total_vector_count(),
                        |id| {
                            VectorElementTypeHalf::slice_from_float_cow(Cow::Borrowed(
                                vector_storage.get_dense(id),
                            ))
                        },
                        None,
                        None,
                    )
                } else {
                    Self::new_typed::<VectorElementType>(
                        device,
                        GpuVectorStorageElementType::Float32,
                        vector_storage.distance(),
                        vector_storage.total_vector_count(),
                        |id| Cow::Borrowed(vector_storage.get_dense(id)),
                        None,
                        None,
                    )
                }
            }
            VectorStorageEnum::DenseAppendableInRamByte(vector_storage) => {
                Self::new_typed::<VectorElementTypeByte>(
                    device,
                    GpuVectorStorageElementType::Uint8,
                    vector_storage.distance(),
                    vector_storage.total_vector_count(),
                    |id| Cow::Borrowed(vector_storage.get_dense(id)),
                    None,
                    None,
                )
            }
            VectorStorageEnum::DenseAppendableInRamHalf(vector_storage) => {
                Self::new_typed::<VectorElementTypeHalf>(
                    device,
                    GpuVectorStorageElementType::Float16,
                    vector_storage.distance(),
                    vector_storage.total_vector_count(),
                    |id| Cow::Borrowed(vector_storage.get_dense(id)),
                    None,
                    None,
                )
            }
            VectorStorageEnum::SparseSimple(_) => Err(gpu::GpuError::NotSupported),
            VectorStorageEnum::MultiDenseSimple(_) => Err(gpu::GpuError::NotSupported),
            VectorStorageEnum::MultiDenseSimpleByte(_) => Err(gpu::GpuError::NotSupported),
            VectorStorageEnum::MultiDenseSimpleHalf(_) => Err(gpu::GpuError::NotSupported),
            VectorStorageEnum::MultiDenseAppendableMemmap(_) => Err(gpu::GpuError::NotSupported),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(_) => {
                Err(gpu::GpuError::NotSupported)
            }
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(_) => {
                Err(gpu::GpuError::NotSupported)
            }
            VectorStorageEnum::MultiDenseAppendableInRam(_) => Err(gpu::GpuError::NotSupported),
            VectorStorageEnum::MultiDenseAppendableInRamByte(_) => Err(gpu::GpuError::NotSupported),
            VectorStorageEnum::MultiDenseAppendableInRamHalf(_) => Err(gpu::GpuError::NotSupported),
        }
    }

    fn new_typed<'a, TElement: PrimitiveVectorElement>(
        device: Arc<gpu::Device>,
        element_type: GpuVectorStorageElementType,
        distance: Distance,
        count: usize,
        get_vector: impl Fn(PointOffsetType) -> Cow<'a, [TElement]>,
        get_sq_offset: Option<Box<dyn Fn(PointOffsetType) -> f32 + 'a>>,
        quantization: Option<GpuQuantizationParams>,
    ) -> gpu::GpuResult<Self> {
        let timer = std::time::Instant::now();

        let dim = get_vector(0).len();

        let capacity = Self::get_capacity(&device, dim);
        let upload_points_count = UPLOAD_CHUNK_SIZE / (capacity * std::mem::size_of::<TElement>());

        let points_in_storage_count = Self::get_points_in_storage_count(count);
        let vectors_buffer: Vec<Arc<gpu::Buffer>> = (0..STORAGES_COUNT)
            .map(|_| -> gpu::GpuResult<Arc<gpu::Buffer>> {
                Ok(Arc::new(gpu::Buffer::new(
                    device.clone(),
                    gpu::BufferType::Storage,
                    points_in_storage_count * capacity * std::mem::size_of::<TElement>(),
                )?))
            })
            .collect::<gpu::GpuResult<Vec<_>>>()?;
        log::trace!("Storage buffer size {}", vectors_buffer[0].size);

        let mut upload_context = gpu::Context::new(device.clone());
        let staging_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::CpuToGpu,
            upload_points_count * capacity * std::mem::size_of::<TElement>(),
        )?);
        log::trace!(
            "Staging buffer size {}, upload_points_count = {}",
            staging_buffer.size,
            upload_points_count
        );

        log::trace!("capacity = {}, count = {}", capacity, count);
        for (storage_index, vector_buffer) in vectors_buffer.iter().enumerate() {
            let mut gpu_offset = 0;
            let mut upload_size = 0;
            let mut upload_points = 0;
            let mut extended_vector = vec![TElement::default(); capacity];
            for point_id in 0..count {
                if point_id % STORAGES_COUNT != storage_index {
                    continue;
                }

                let vector = get_vector(point_id as PointOffsetType);
                extended_vector[..vector.len()].copy_from_slice(&vector);
                staging_buffer.upload_slice(
                    &extended_vector,
                    upload_points * capacity * std::mem::size_of::<TElement>(),
                );
                upload_size += capacity * std::mem::size_of::<TElement>();
                upload_points += 1;

                if upload_points == upload_points_count {
                    upload_context.copy_gpu_buffer(
                        staging_buffer.clone(),
                        vector_buffer.clone(),
                        0,
                        gpu_offset,
                        upload_size,
                    );
                    upload_context.run();
                    upload_context.wait_finish();

                    log::trace!(
                        "Uploaded {} vectors, {} MB",
                        upload_points,
                        upload_size / 1024 / 1024,
                    );

                    gpu_offset += upload_size;
                    upload_size = 0;
                    upload_points = 0;
                }
            }
            if upload_points > 0 {
                upload_context.copy_gpu_buffer(
                    staging_buffer.clone(),
                    vectors_buffer[storage_index].clone(),
                    0,
                    gpu_offset,
                    upload_size,
                );
                upload_context.run();
                upload_context.wait_finish();

                log::trace!(
                    "Uploaded {} vectors, {} MB",
                    upload_points,
                    upload_size / 1024 / 1024,
                );
            }
        }

        log::trace!(
            "Upload vector data to GPU time = {:?}, vector data size {} MB, element type: {:?}",
            timer.elapsed(),
            STORAGES_COUNT * points_in_storage_count * capacity * std::mem::size_of::<TElement>()
                / 1024
                / 1024,
            element_type,
        );

        let sq_offsets_buffer = if let Some(offsets_fn) = get_sq_offset {
            let sq_offsets_buffer = Arc::new(gpu::Buffer::new(
                device.clone(),
                gpu::BufferType::Storage,
                count * std::mem::size_of::<f32>(),
            )?);
            for (chunk_index, chunk) in (0..count as PointOffsetType)
                .chunks(upload_points_count)
                .into_iter()
                .enumerate()
            {
                for (buffer_offset, idx) in chunk.enumerate() {
                    let offset = offsets_fn(idx);
                    staging_buffer.upload(&offset, buffer_offset * std::mem::size_of::<f32>());
                }
                let dst_offset = chunk_index * upload_points_count * std::mem::size_of::<f32>();
                let copy_size = (upload_points_count * std::mem::size_of::<f32>())
                    .min(sq_offsets_buffer.size - dst_offset);
                upload_context.copy_gpu_buffer(
                    staging_buffer.clone(),
                    sq_offsets_buffer.clone(),
                    0,
                    chunk_index * upload_points_count * std::mem::size_of::<f32>(),
                    copy_size,
                );
                upload_context.run();
                upload_context.wait_finish();
            }
            Some(sq_offsets_buffer)
        } else {
            None
        };

        let mut descriptor_set_layout_builder = gpu::DescriptorSetLayout::builder();
        for i in 0..STORAGES_COUNT {
            descriptor_set_layout_builder = descriptor_set_layout_builder.add_storage_buffer(i);
        }
        if sq_offsets_buffer.is_some() {
            descriptor_set_layout_builder =
                descriptor_set_layout_builder.add_storage_buffer(STORAGES_COUNT);
        }
        let descriptor_set_layout = descriptor_set_layout_builder.build(device.clone());

        let mut descriptor_set_builder = gpu::DescriptorSet::builder(descriptor_set_layout.clone());
        for (i, vector_buffer) in vectors_buffer.iter().enumerate() {
            descriptor_set_builder =
                descriptor_set_builder.add_storage_buffer(i, vector_buffer.clone());
        }
        if let Some(sq_offsets_buffer) = &sq_offsets_buffer {
            descriptor_set_builder = descriptor_set_builder
                .add_storage_buffer(STORAGES_COUNT, sq_offsets_buffer.clone());
        }
        let descriptor_set = descriptor_set_builder.build();

        Ok(Self {
            device,
            vectors_buffer,
            sq_offsets_buffer,
            descriptor_set_layout,
            descriptor_set,
            dim: capacity,
            count,
            element_type,
            distance,
            quantization,
        })
    }

    pub fn get_capacity(device: &Arc<gpu::Device>, dim: usize) -> usize {
        let alignment = device.subgroup_size * ELEMENTS_PER_SUBGROUP;
        dim + (alignment - dim % alignment) % alignment
    }

    pub fn get_points_in_storage_count(num_vectors: usize) -> usize {
        (num_vectors + (STORAGES_COUNT - num_vectors % STORAGES_COUNT)) / STORAGES_COUNT
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use bitvec::vec::BitVec;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    use super::*;
    use crate::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};
    use crate::fixtures::index_fixtures::random_vector;
    use crate::fixtures::payload_fixtures::random_dense_byte_vector;
    use crate::index::hnsw_index::gpu::shader_builder::ShaderBuilder;
    use crate::spaces::metric::Metric;
    use crate::spaces::simple::DotProductMetric;
    use crate::types::{
        BinaryQuantization, BinaryQuantizationConfig, Distance, QuantizationConfig,
        ScalarQuantization, ScalarQuantizationConfig,
    };
    use crate::vector_storage::dense::simple_dense_vector_storage::{
        open_simple_dense_byte_vector_storage, open_simple_dense_half_vector_storage,
        open_simple_dense_vector_storage,
    };

    enum TestElementType {
        Float32,
        Float16,
        Uint8,
    }

    fn open_vector_storage(
        path: &Path,
        dim: usize,
        element_type: TestElementType,
    ) -> VectorStorageEnum {
        let db = open_db(path, &[DB_VECTOR_CF]).unwrap();

        match element_type {
            TestElementType::Float32 => open_simple_dense_vector_storage(
                db,
                DB_VECTOR_CF,
                dim,
                Distance::Dot,
                &false.into(),
            )
            .unwrap(),
            TestElementType::Float16 => open_simple_dense_half_vector_storage(
                db,
                DB_VECTOR_CF,
                dim,
                Distance::Dot,
                &false.into(),
            )
            .unwrap(),
            TestElementType::Uint8 => open_simple_dense_byte_vector_storage(
                db,
                DB_VECTOR_CF,
                dim,
                Distance::Dot,
                &false.into(),
            )
            .unwrap(),
        }
    }

    fn test_gpu_vector_storage_scoring_impl(
        element_type: TestElementType,
        force_half_precision: bool,
    ) -> GpuVectorStorageElementType {
        let num_vectors = 2048;
        let dim = 128;
        let test_point_id = 0usize;

        let mut rnd = StdRng::seed_from_u64(42);
        let points = (0..num_vectors)
            .map(|_| match element_type {
                TestElementType::Float32 => random_vector(&mut rnd, dim),
                TestElementType::Float16 => random_vector(&mut rnd, dim),
                TestElementType::Uint8 => random_dense_byte_vector(&mut rnd, dim),
            })
            .collect::<Vec<_>>();

        let dir = tempfile::Builder::new().prefix("db_dir").tempdir().unwrap();
        let mut storage = open_vector_storage(dir.path(), dim, element_type);
        points.iter().enumerate().for_each(|(i, vec)| {
            storage
                .insert_vector(i as PointOffsetType, vec.into())
                .unwrap();
        });

        let debug_messenger = gpu::PanicIfErrorMessenger {};
        let instance =
            Arc::new(gpu::Instance::new("qdrant", Some(&debug_messenger), false).unwrap());
        let device = Arc::new(
            gpu::Device::new(instance.clone(), instance.vk_physical_devices[0].clone()).unwrap(),
        );

        let gpu_vector_storage =
            GpuVectorStorage::new(device.clone(), &storage, None, force_half_precision).unwrap();

        let scores_buffer = Arc::new(
            gpu::Buffer::new(
                device.clone(),
                gpu::BufferType::Storage,
                num_vectors * std::mem::size_of::<f32>(),
            )
            .unwrap(),
        );

        let descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_storage_buffer(0)
            .build(device.clone());

        let descriptor_set = gpu::DescriptorSet::builder(descriptor_set_layout.clone())
            .add_storage_buffer(0, scores_buffer.clone())
            .build();

        let shader = ShaderBuilder::new(device.clone())
            .with_shader_code(include_str!("shaders/tests/test_vector_storage.comp"))
            .with_gpu_vector_storage(&gpu_vector_storage)
            .build();

        let pipeline = gpu::Pipeline::builder()
            .add_descriptor_set_layout(0, descriptor_set_layout.clone())
            .add_descriptor_set_layout(1, gpu_vector_storage.descriptor_set_layout.clone())
            .add_shader(shader.clone())
            .build(device.clone());

        let mut context = gpu::Context::new(device.clone());
        context.bind_pipeline(
            pipeline,
            &[descriptor_set, gpu_vector_storage.descriptor_set.clone()],
        );
        context.dispatch(num_vectors, 1, 1);

        let timer = std::time::Instant::now();
        context.run();
        context.wait_finish();
        log::trace!("GPU scoring time = {:?}", timer.elapsed());

        let staging_buffer = Arc::new(
            gpu::Buffer::new(
                device.clone(),
                gpu::BufferType::GpuToCpu,
                num_vectors * std::mem::size_of::<f32>(),
            )
            .unwrap(),
        );
        context.copy_gpu_buffer(
            scores_buffer,
            staging_buffer.clone(),
            0,
            0,
            num_vectors * std::mem::size_of::<f32>(),
        );
        context.run();
        context.wait_finish();

        let mut scores = vec![0.0f32; num_vectors];
        staging_buffer.download_slice(&mut scores, 0);

        let timer = std::time::Instant::now();
        for i in 0..num_vectors {
            let score = DotProductMetric::similarity(&points[test_point_id], &points[i]);
            assert!((score - scores[i]).abs() < 0.01);
        }
        log::trace!("CPU scoring time = {:?}", timer.elapsed());

        gpu_vector_storage.element_type
    }

    #[test]
    fn test_gpu_vector_storage_scoring() {
        let _ = env_logger::builder()
            .is_test(true)
            .filter_level(log::LevelFilter::Trace)
            .try_init();
        let element = test_gpu_vector_storage_scoring_impl(TestElementType::Float32, false);
        assert_eq!(element, GpuVectorStorageElementType::Float32);
    }

    #[test]
    fn test_gpu_vector_storage_scoring_f16() {
        let _ = env_logger::builder()
            .is_test(true)
            .filter_level(log::LevelFilter::Trace)
            .try_init();
        let element = test_gpu_vector_storage_scoring_impl(TestElementType::Float16, false);
        assert_eq!(element, GpuVectorStorageElementType::Float16);
    }

    #[test]
    fn test_gpu_vector_storage_scoring_u8() {
        let _ = env_logger::builder()
            .is_test(true)
            .filter_level(log::LevelFilter::Trace)
            .try_init();
        let element = test_gpu_vector_storage_scoring_impl(TestElementType::Uint8, false);
        assert_eq!(element, GpuVectorStorageElementType::Uint8);
    }

    #[test]
    fn test_gpu_vector_storage_force_half_precision() {
        let element = test_gpu_vector_storage_scoring_impl(TestElementType::Float32, true);
        assert_eq!(element, GpuVectorStorageElementType::Float16);
    }

    #[test]
    fn test_gpu_vector_storage_binary_quantization() {
        let _ = env_logger::builder()
            .is_test(true)
            .filter_level(log::LevelFilter::Trace)
            .try_init();

        let num_vectors = 16;
        let dim = 1024;
        let test_point_id = 0usize;

        let mut rnd = StdRng::seed_from_u64(42);
        let points = (0..num_vectors)
            .map(|_| random_vector(&mut rnd, dim))
            .collect::<Vec<_>>();

        let dir = tempfile::Builder::new().prefix("db_dir").tempdir().unwrap();
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
        let mut storage =
            open_simple_dense_vector_storage(db, DB_VECTOR_CF, dim, Distance::Dot, &false.into())
                .unwrap();

        points.iter().enumerate().for_each(|(i, vec)| {
            storage
                .insert_vector(i as PointOffsetType, vec.into())
                .unwrap();
        });

        let quantized_vectors = QuantizedVectors::create(
            &storage,
            &QuantizationConfig::Binary(BinaryQuantization {
                binary: BinaryQuantizationConfig {
                    always_ram: Some(true),
                },
            }),
            dir.path(),
            1,
            &false.into(),
        )
        .unwrap();

        let debug_messenger = gpu::PanicIfErrorMessenger {};
        let instance =
            Arc::new(gpu::Instance::new("qdrant", Some(&debug_messenger), false).unwrap());
        let device = Arc::new(
            gpu::Device::new(instance.clone(), instance.vk_physical_devices[0].clone()).unwrap(),
        );

        let gpu_vector_storage =
            GpuVectorStorage::new(device.clone(), &storage, Some(&quantized_vectors), false)
                .unwrap();
        assert_eq!(
            gpu_vector_storage.element_type,
            GpuVectorStorageElementType::Binary
        );

        let scores_buffer = Arc::new(
            gpu::Buffer::new(
                device.clone(),
                gpu::BufferType::Storage,
                num_vectors * std::mem::size_of::<f32>(),
            )
            .unwrap(),
        );

        let descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_storage_buffer(0)
            .build(device.clone());

        let descriptor_set = gpu::DescriptorSet::builder(descriptor_set_layout.clone())
            .add_storage_buffer(0, scores_buffer.clone())
            .build();

        let shader = ShaderBuilder::new(device.clone())
            .with_shader_code(include_str!("shaders/tests/test_vector_storage.comp"))
            .with_gpu_vector_storage(&gpu_vector_storage)
            .build();

        let pipeline = gpu::Pipeline::builder()
            .add_descriptor_set_layout(0, descriptor_set_layout.clone())
            .add_descriptor_set_layout(1, gpu_vector_storage.descriptor_set_layout.clone())
            .add_shader(shader.clone())
            .build(device.clone());

        let mut context = gpu::Context::new(device.clone());
        context.bind_pipeline(
            pipeline,
            &[descriptor_set, gpu_vector_storage.descriptor_set.clone()],
        );
        context.dispatch(num_vectors, 1, 1);

        let timer = std::time::Instant::now();
        context.run();
        context.wait_finish();
        log::trace!("GPU scoring time = {:?}", timer.elapsed());

        let staging_buffer = Arc::new(
            gpu::Buffer::new(
                device.clone(),
                gpu::BufferType::GpuToCpu,
                num_vectors * std::mem::size_of::<f32>(),
            )
            .unwrap(),
        );
        context.copy_gpu_buffer(
            scores_buffer,
            staging_buffer.clone(),
            0,
            0,
            num_vectors * std::mem::size_of::<f32>(),
        );
        context.run();
        context.wait_finish();

        let mut scores = vec![0.0f32; num_vectors];
        staging_buffer.download_slice(&mut scores, 0);

        let stopped = false.into();
        let point_deleted = BitVec::repeat(false, num_vectors);
        let scorer = quantized_vectors
            .raw_scorer(
                points[test_point_id].clone().into(),
                &point_deleted,
                &point_deleted,
                &stopped,
            )
            .unwrap();
        for i in 0..num_vectors {
            let score =
                scorer.score_internal(test_point_id as PointOffsetType, i as PointOffsetType);
            assert_eq!(score, scores[i]);
        }
    }

    #[test]
    fn test_gpu_vector_storage_sq() {
        let _ = env_logger::builder()
            .is_test(true)
            .filter_level(log::LevelFilter::Trace)
            .try_init();

        let num_vectors = 16;
        let dim = 1024;
        let test_point_id = 0usize;

        let mut rnd = StdRng::seed_from_u64(42);
        let points = (0..num_vectors)
            .map(|_| random_vector(&mut rnd, dim))
            .collect::<Vec<_>>();

        let dir = tempfile::Builder::new().prefix("db_dir").tempdir().unwrap();
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
        let mut storage =
            open_simple_dense_vector_storage(db, DB_VECTOR_CF, dim, Distance::Dot, &false.into())
                .unwrap();

        points.iter().enumerate().for_each(|(i, vec)| {
            storage
                .insert_vector(i as PointOffsetType, vec.into())
                .unwrap();
        });

        let quantized_vectors = QuantizedVectors::create(
            &storage,
            &QuantizationConfig::Scalar(ScalarQuantization {
                scalar: ScalarQuantizationConfig {
                    always_ram: Some(true),
                    r#type: crate::types::ScalarType::Int8,
                    quantile: Some(0.99),
                },
            }),
            dir.path(),
            1,
            &false.into(),
        )
        .unwrap();

        let debug_messenger = gpu::PanicIfErrorMessenger {};
        let instance =
            Arc::new(gpu::Instance::new("qdrant", Some(&debug_messenger), false).unwrap());
        let device = Arc::new(
            gpu::Device::new(instance.clone(), instance.vk_physical_devices[0].clone()).unwrap(),
        );

        let gpu_vector_storage =
            GpuVectorStorage::new(device.clone(), &storage, Some(&quantized_vectors), false)
                .unwrap();
        assert_eq!(
            gpu_vector_storage.element_type,
            GpuVectorStorageElementType::SQ
        );

        let scores_buffer = Arc::new(
            gpu::Buffer::new(
                device.clone(),
                gpu::BufferType::Storage,
                num_vectors * std::mem::size_of::<f32>(),
            )
            .unwrap(),
        );

        let descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_storage_buffer(0)
            .build(device.clone());

        let descriptor_set = gpu::DescriptorSet::builder(descriptor_set_layout.clone())
            .add_storage_buffer(0, scores_buffer.clone())
            .build();

        let shader = ShaderBuilder::new(device.clone())
            .with_shader_code(include_str!("shaders/tests/test_vector_storage.comp"))
            .with_gpu_vector_storage(&gpu_vector_storage)
            .build();

        let pipeline = gpu::Pipeline::builder()
            .add_descriptor_set_layout(0, descriptor_set_layout.clone())
            .add_descriptor_set_layout(1, gpu_vector_storage.descriptor_set_layout.clone())
            .add_shader(shader.clone())
            .build(device.clone());

        let mut context = gpu::Context::new(device.clone());
        context.bind_pipeline(
            pipeline,
            &[descriptor_set, gpu_vector_storage.descriptor_set.clone()],
        );
        context.dispatch(num_vectors, 1, 1);

        let timer = std::time::Instant::now();
        context.run();
        context.wait_finish();
        log::trace!("GPU scoring time = {:?}", timer.elapsed());

        let staging_buffer = Arc::new(
            gpu::Buffer::new(
                device.clone(),
                gpu::BufferType::GpuToCpu,
                num_vectors * std::mem::size_of::<f32>(),
            )
            .unwrap(),
        );
        context.copy_gpu_buffer(
            scores_buffer,
            staging_buffer.clone(),
            0,
            0,
            num_vectors * std::mem::size_of::<f32>(),
        );
        context.run();
        context.wait_finish();

        let mut scores = vec![0.0f32; num_vectors];
        staging_buffer.download_slice(&mut scores, 0);

        let stopped = false.into();
        let point_deleted = BitVec::repeat(false, num_vectors);
        let scorer = quantized_vectors
            .raw_scorer(
                points[test_point_id].clone().into(),
                &point_deleted,
                &point_deleted,
                &stopped,
            )
            .unwrap();
        for i in 0..num_vectors {
            let score =
                scorer.score_internal(test_point_id as PointOffsetType, i as PointOffsetType);
            assert!((score - scores[i]).abs() < 0.01);
        }
    }
}
