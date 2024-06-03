#!/bin/bash

set -ex

glslc test_vector_storage.comp -o compiled/test_vector_storage.spv -O --target-spv=spv1.3 --target-env=vulkan1.3
glslc test_links.comp -o compiled/test_links.spv -O --target-spv=spv1.3 --target-env=vulkan1.3
glslc test_searcher.comp -o compiled/test_searcher.spv -O --target-spv=spv1.3 --target-env=vulkan1.3
glslc test_greedy_searcher.comp -o compiled/test_greedy_searcher.spv -O --target-spv=spv1.3 --target-env=vulkan1.3
glslc test_heuristic.comp -o compiled/test_heuristic.spv -O --target-spv=spv1.3 --target-env=vulkan1.3
glslc test_nearest_heap.comp -o compiled/test_nearest_heap.spv -O --target-spv=spv1.3 --target-env=vulkan1.3
glslc test_candidates_heap.comp -o compiled/test_candidates_heap.spv -O --target-spv=spv1.3 --target-env=vulkan1.3
glslc test_visited_flags.comp -o compiled/test_visited_flags.spv -O --target-spv=spv1.3 --target-env=vulkan1.3
glslc run_requests.comp -o compiled/run_requests.spv -O --target-spv=spv1.3 --target-env=vulkan1.3
glslc update_entries.comp -o compiled/update_entries.spv -O --target-spv=spv1.3 --target-env=vulkan1.3
glslc builder_profile_helper.comp -o compiled/builder_profile_helper.spv -O --target-spv=spv1.3 --target-env=vulkan1.3
