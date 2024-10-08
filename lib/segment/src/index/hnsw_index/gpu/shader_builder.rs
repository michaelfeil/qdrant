use std::collections::HashMap;
use std::sync::Arc;

use super::gpu_vector_storage::GpuVectorStorageElementType;
use crate::types::Distance;

pub struct ShaderBuilder {
    device: Arc<gpu::Device>,
    shader_code: String,
    element_type: Option<GpuVectorStorageElementType>,
    distance: Option<Distance>,
    sq_multiplier: Option<f32>,
    sq_diff: Option<f32>,
    exact: Option<bool>,
    dim: Option<usize>,
    storages_count: Option<usize>,
    storage_size: Option<usize>,
    nearest_heap_ef: Option<usize>,
    nearest_heap_capacity: Option<usize>,
    candidates_heap_capacity: Option<usize>,
    links_capacity: Option<usize>,
    visited_flags_capacity: Option<usize>,
    shaders_map: HashMap<String, String>,
}

impl ShaderBuilder {
    pub fn new(device: Arc<gpu::Device>) -> Self {
        let shaders_map = HashMap::from([
            (
                "bheap.comp".to_string(),
                include_str!("shaders/bheap.comp").to_string(),
            ),
            (
                "candidates_heap.comp".to_string(),
                include_str!("shaders/candidates_heap.comp").to_string(),
            ),
            (
                "common.comp".to_string(),
                include_str!("shaders/common.comp").to_string(),
            ),
            (
                "extensions.comp".to_string(),
                include_str!("shaders/extensions.comp").to_string(),
            ),
            (
                "iterators.comp".to_string(),
                include_str!("shaders/iterators.comp").to_string(),
            ),
            (
                "links.comp".to_string(),
                include_str!("shaders/links.comp").to_string(),
            ),
            (
                "nearest_heap.comp".to_string(),
                include_str!("shaders/nearest_heap.comp").to_string(),
            ),
            (
                "run_get_patch.comp".to_string(),
                include_str!("shaders/run_get_patch.comp").to_string(),
            ),
            (
                "run_greedy_search.comp".to_string(),
                include_str!("shaders/run_greedy_search.comp").to_string(),
            ),
            (
                "run_insert_vector.comp".to_string(),
                include_str!("shaders/run_insert_vector.comp").to_string(),
            ),
            (
                "search_context.comp".to_string(),
                include_str!("shaders/search_context.comp").to_string(),
            ),
            (
                "vector_storage.comp".to_string(),
                include_str!("shaders/vector_storage.comp").to_string(),
            ),
            (
                "vector_storage_bq.comp".to_string(),
                include_str!("shaders/vector_storage_bq.comp").to_string(),
            ),
            (
                "vector_storage_f16.comp".to_string(),
                include_str!("shaders/vector_storage_f16.comp").to_string(),
            ),
            (
                "vector_storage_f32.comp".to_string(),
                include_str!("shaders/vector_storage_f32.comp").to_string(),
            ),
            (
                "vector_storage_sq.comp".to_string(),
                include_str!("shaders/vector_storage_sq.comp").to_string(),
            ),
            (
                "vector_storage_u8.comp".to_string(),
                include_str!("shaders/vector_storage_u8.comp").to_string(),
            ),
            (
                "visited_flags.comp".to_string(),
                include_str!("shaders/visited_flags.comp").to_string(),
            ),
        ]);

        Self {
            device,
            shader_code: Default::default(),
            element_type: None,
            distance: None,
            sq_multiplier: None,
            sq_diff: None,
            exact: None,
            dim: None,
            storages_count: None,
            storage_size: None,
            nearest_heap_ef: None,
            nearest_heap_capacity: None,
            candidates_heap_capacity: None,
            links_capacity: None,
            visited_flags_capacity: None,
            shaders_map,
        }
    }

    pub fn with_shader_code(&mut self, shader_code: &str) -> &mut Self {
        self.shader_code.push_str("\n");
        self.shader_code.push_str(shader_code);
        self
    }

    pub fn with_element_type(&mut self, element_type: GpuVectorStorageElementType) -> &mut Self {
        self.element_type = Some(element_type);
        self
    }

    pub fn with_distance(&mut self, distance: Distance) -> &mut Self {
        self.distance = Some(distance);
        self
    }

    pub fn with_sq_multiplier(&mut self, sq_multiplier: Option<f32>) -> &mut Self {
        self.sq_multiplier = sq_multiplier;
        self
    }

    pub fn with_sq_diff(&mut self, sq_diff: Option<f32>) -> &mut Self {
        self.sq_diff = sq_diff;
        self
    }

    pub fn with_exact(&mut self, exact: bool) -> &mut Self {
        self.exact = Some(exact);
        self
    }

    pub fn with_dim(&mut self, dim: usize) -> &mut Self {
        self.dim = Some(dim);
        self
    }

    pub fn with_storages_count(&mut self, storages_count: usize) -> &mut Self {
        self.storages_count = Some(storages_count);
        self
    }

    pub fn with_storage_size(&mut self, storage_size: usize) -> &mut Self {
        self.storage_size = Some(storage_size);
        self
    }

    pub fn with_nearest_heap_ef(&mut self, nearest_heap_ef: usize) -> &mut Self {
        self.nearest_heap_ef = Some(nearest_heap_ef);
        self
    }

    pub fn with_nearest_heap_capacity(&mut self, nearest_heap_capacity: usize) -> &mut Self {
        self.nearest_heap_capacity = Some(nearest_heap_capacity);
        self
    }

    pub fn with_candidates_heap_capacity(&mut self, candidates_heap_capacity: usize) -> &mut Self {
        self.candidates_heap_capacity = Some(candidates_heap_capacity);
        self
    }

    pub fn with_links_capacity(&mut self, links_capacity: usize) -> &mut Self {
        self.links_capacity = Some(links_capacity);
        self
    }

    pub fn with_visited_flags_capacity(&mut self, visited_flags_capacity: usize) -> &mut Self {
        self.visited_flags_capacity = Some(visited_flags_capacity);
        self
    }

    pub fn build(&self) -> Arc<gpu::Shader> {
        let mut options = shaderc::CompileOptions::new().unwrap();
        options.set_optimization_level(shaderc::OptimizationLevel::Performance);
        options.set_target_env(
            shaderc::TargetEnv::Vulkan,
            shaderc::EnvVersion::Vulkan1_3 as u32,
        );
        options.set_target_spirv(shaderc::SpirvVersion::V1_3);

        options.add_macro_definition(
            "SUBGROUP_SIZE",
            Some(&self.device.subgroup_size().to_string()),
        );

        if let Some(element_type) = self.element_type {
            match element_type {
                GpuVectorStorageElementType::Float32 => {
                    options.add_macro_definition("VECTOR_STORAGE_ELEMENT_FLOAT32", None)
                }
                GpuVectorStorageElementType::Float16 => {
                    options.add_macro_definition("VECTOR_STORAGE_ELEMENT_FLOAT16", None)
                }
                GpuVectorStorageElementType::Uint8 => {
                    options.add_macro_definition("VECTOR_STORAGE_ELEMENT_UINT8", None)
                }
                GpuVectorStorageElementType::Binary => {
                    options.add_macro_definition("VECTOR_STORAGE_ELEMENT_BINARY", None)
                }
                GpuVectorStorageElementType::SQ => {
                    options.add_macro_definition("VECTOR_STORAGE_ELEMENT_SQ", None)
                }
            }
        }

        if let Some(distance) = self.distance {
            match distance {
                Distance::Cosine => options.add_macro_definition("COSINE_DISTANCE", None),
                Distance::Euclid => options.add_macro_definition("EUCLID_DISTANCE", None),
                Distance::Dot => options.add_macro_definition("DOT_DISTANCE", None),
                Distance::Manhattan => options.add_macro_definition("MANHATTAN_DISTANCE", None),
            }
        }

        if let Some(sq_multiplier) = self.sq_multiplier {
            options.add_macro_definition("SQ_MULTIPLIER", Some(&sq_multiplier.to_string()));
        }

        if let Some(sq_diff) = self.sq_diff {
            options.add_macro_definition("SQ_DIFF", Some(&sq_diff.to_string()));
        }

        if self.exact == Some(true) {
            options.add_macro_definition("EXACT", None);
        }

        if let Some(dim) = self.dim {
            options.add_macro_definition("DIM", Some(&dim.to_string()));
        }

        if let Some(storages_count) = self.storages_count {
            options.add_macro_definition("STORAGES_COUNT", Some(&storages_count.to_string()));
        }

        if let Some(storage_size) = self.storage_size {
            options.add_macro_definition("STORAGE_SIZE", Some(&storage_size.to_string()));
        }

        if let Some(nearest_heap_ef) = self.nearest_heap_ef {
            options.add_macro_definition("NEAREST_HEAP_EF", Some(&nearest_heap_ef.to_string()));
        }

        if let Some(nearest_heap_capacity) = self.nearest_heap_capacity {
            options.add_macro_definition(
                "NEAREST_HEAP_CAPACITY",
                Some(&nearest_heap_capacity.to_string()),
            );
        }

        if let Some(candidates_heap_capacity) = self.candidates_heap_capacity {
            options.add_macro_definition(
                "CANDIDATES_HEAP_CAPACITY",
                Some(&candidates_heap_capacity.to_string()),
            );
        }

        if let Some(links_capacity) = self.links_capacity {
            options.add_macro_definition("LINKS_CAPACITY", Some(&links_capacity.to_string()));
        }

        if let Some(visited_flags_capacity) = self.visited_flags_capacity {
            options.add_macro_definition(
                "VISITED_FLAGS_CAPACITY",
                Some(&visited_flags_capacity.to_string()),
            );
        }

        options.set_include_callback(|filename, _, _, _| {
            let code = self.shaders_map.get(filename).unwrap();
            Ok(shaderc::ResolvedInclude {
                resolved_name: filename.to_string(),
                content: code.to_owned(),
            })
        });

        let timer = std::time::Instant::now();
        let compiled = self
            .device
            .compiler
            .compile_into_spirv(
                &self.shader_code,
                shaderc::ShaderKind::Compute,
                "shader.glsl",
                "main",
                Some(&options),
            )
            .unwrap();
        log::debug!("Shader compilation took: {:?}", timer.elapsed());
        Arc::new(gpu::Shader::new(
            self.device.clone(),
            compiled.as_binary_u8(),
        ))
    }
}
