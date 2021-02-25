cmake_minimum_required(VERSION 3.11)

project(voila)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
	message(FATAL_ERROR "No build type was specified. Valid choices are either -DCMAKE_BUILD_TYPE=Release or -DCMAKE_BUILD_TYPE=Debug")
endif()

set(ARCH_FLAGS "-mtune=native")
if(${CMAKE_SYSTEM_PROCESSOR} MATCHES "ppc64")
  # https://github.com/mfem/mfem/issues/216
  # march=native not supported on PPC
  set(ARCH_FLAGS "${ARCH_FLAGS} -mcpu=native")
else()
  set(ARCH_FLAGS "${ARCH_FLAGS} -march=native")
endif()


# Download and unpack googletest at configure time
configure_file(db-engine-paradigms/3rdparty/GTest.CMakeLists.txt googletest-download/CMakeLists.txt)
execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" .
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/googletest-download" )
execute_process(COMMAND "${CMAKE_COMMAND}" --build .
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/googletest-download" )

# Prevent GoogleTest from overriding our compiler/linker options
# when building with Visual Studio
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

set(CMAKE_CXX_FLAGS "-Wno-deprecated-copy")

# Add googletest directly to our build. This adds
# the following targets: gtest, gtest_main, gmock
# and gmock_main
add_subdirectory("${CMAKE_BINARY_DIR}/googletest-src"
                 "${CMAKE_BINARY_DIR}/googletest-build")

# The gtest/gmock targets carry header search path
# dependencies automatically when using CMake 2.8.11 or
# later. Otherwise we have to add them here ourselves.
if(CMAKE_VERSION VERSION_LESS 2.8.11)
    include_directories("${gtest_SOURCE_DIR}/include"
                        "${gmock_SOURCE_DIR}/include")
endif()


set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/")
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/db-engine-paradigms/3rdparty/")

include_directories(db-engine-paradigms/3rdparty/)
include_directories(db-engine-paradigms/3rdparty/jevents)


add_subdirectory(db-engine-paradigms)

include_directories(db-engine-paradigms/include)

set(CMAKE_CXX_FLAGS " ${ARCH_FLAGS} -g -fPIC -Wall -Wextra --std=c++17 -Wno-type-limits")

set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG}  ${ARCH_FLAGS} -fno-omit-frame-pointer -fsanitize=address  -DIS_DEBUG")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${ARCH_FLAGS} -fno-omit-frame-pointer -fsanitize=address  -DIS_DEBUG")

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} ${ARCH_FLAGS} -O3 -g -fno-omit-frame-pointer -DIS_RELEASE")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} ${ARCH_FLAGS} -O3 -g -fno-omit-frame-pointer -DIS_RELEASE")
set(CMAKE_LINKER_FLAGS_RELEASE "${CMAKE_LINKER_FLAGS_RELEASE} ${ARCH_FLAGS} -O3 -g -fno-omit-frame-pointer -DIS_RELEASE")

find_package(PythonInterp 2.7 REQUIRED)

set(NUM_PARTITIONS 32)

foreach(i RANGE ${NUM_PARTITIONS})
	if(${i} GREATER 0)
		set(H_FILE "${CMAKE_CURRENT_BINARY_DIR}/kernels${i}.hpp")
		set(C_FILE "${CMAKE_CURRENT_BINARY_DIR}/kernels${i}.cpp")

		add_custom_command(
			COMMAND PYTHONHASHSEED=2 ${PYTHON_EXECUTABLE} "-B" "-O" "${CMAKE_CURRENT_SOURCE_DIR}/kernel_generator.py" "--h=${H_FILE}" "--c=${C_FILE}" ${ARGS} "--p=${i}" "--n=${NUM_PARTITIONS}"
			DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/kernel_generator.py
			OUTPUT ${H_FILE} ${C_FILE}
			COMMENT "Generating kernels ${i}/${NUM_PARTITIONS}"
			)
		set_source_files_properties(${H_FILE} ${C_FILE} GENERATED)
		list(APPEND GENERATED_KERNELS "${C_FILE}")
	endif()
endforeach(i)

include_directories(${CMAKE_CURRENT_BINARY_DIR})

# add_subdirectory(libs/tbb)

find_package(TBB)
include_directories(${TBB_INCLUDE_DIRS})

add_definitions(${TBB_DEFINITIONS})

include(CheckSymbolExists)

CHECK_SYMBOL_EXISTS(mmap "sys/mman.h" HAVE_POSIX_MMAP)
CHECK_SYMBOL_EXISTS(mremap "sys/mman.h" HAVE_LINUX_MREMAP)
CHECK_SYMBOL_EXISTS(sysconf "unistd.h" HAVE_POSIX_SYSCONF)
CHECK_SYMBOL_EXISTS(MAP_POPULATE "sys/mman.h" HAVE_LINUX_MAP_POPULATE)

configure_file(run_explore_base_flavor.py run_explore_base_flavor.py COPYONLY)
configure_file(run_explore_pipeline_flavor.py run_explore_pipeline_flavor.py COPYONLY)
configure_file(run_related_work_imv.py run_related_work_imv.py COPYONLY)
configure_file(run_perf_baseline.py run_perf_baseline.py COPYONLY)
configure_file(voila_tools.py voila_tools.py COPYONLY)
configure_file(run_all.py run_all.py COPYONLY)

configure_file(print_base_flavors.py print_base_flavors.py COPYONLY)
configure_file(print_code_metrics.py print_code_metrics.py COPYONLY)
configure_file(print_perf_baseline.py print_perf_baseline.py COPYONLY)
configure_file(print_related_work.py print_related_work.py COPYONLY)
configure_file(print_all_runs.sh print_all_runs.sh COPYONLY)
configure_file(print_explore_pipeline_flavors.py print_explore_pipeline_flavors.py COPYONLY)
configure_file(print_explore_blends.py print_explore_blends.py COPYONLY)
configure_file(test_tpch.py test_tpch.py COPYONLY)
configure_file(build_config.py build_config.py)
configure_file(gen_tables.py gen_tables.py COPYONLY)

configure_file(runtime.hpp runtime.hpp COPYONLY)
configure_file(runtime_utils.hpp runtime_utils.hpp COPYONLY)
configure_file(runtime_vector.hpp runtime_vector.hpp COPYONLY)
configure_file(runtime_simd.hpp runtime_simd.hpp COPYONLY)
configure_file(runtime_translation.hpp runtime_translation.hpp COPYONLY)
configure_file(runtime_buffering.hpp runtime_buffering.hpp COPYONLY)
configure_file(runtime_struct.hpp runtime_struct.hpp COPYONLY)
configure_file(runtime_framework.hpp runtime_framework.hpp COPYONLY)
configure_file(runtime_hyper.hpp runtime_hyper.hpp COPYONLY)

configure_file(test1.cpp test1.cpp COPYONLY)
configure_file(test_env.sh test_env.sh)
configure_file(build.hpp.in ${CMAKE_CURRENT_BINARY_DIR}/build.hpp)
configure_file(build.cpp.in ${CMAKE_CURRENT_BINARY_DIR}/build.cpp)
configure_file(test.sh test.sh COPYONLY)
configure_file(generate_tpch.sh generate_tpch.sh COPYONLY)

configure_file(debug70.sh debug70.sh COPYONLY)
configure_file(debug78.sh debug78.sh COPYONLY)

find_package(Threads REQUIRED)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/libs)

add_library(voila_runtime SHARED runtime.cpp runtime_vector.cpp runtime_memory.cpp runtime_framework.cpp runtime_hyper.cpp runtime_utils.cpp runtime_struct.cpp runtime_simd.cpp utils.cpp ${GENERATED_KERNELS} ${CMAKE_CURRENT_BINARY_DIR}//build.cpp sqlite3.c)
target_link_libraries(voila_runtime common ${CMAKE_DL_LIBS} ${CMAKE_THREAD_LIBS_INIT} ${TBB_LIBRARIES}  ${TBB_IMPORTED_TARGETS} ${TBB_LIBRARIES_RELEASE})

add_library(voila_compiler STATIC relalg.cpp relalg_translator.cpp codegen.cpp voila.cpp blend_space_point.cpp cg_hyper.cpp blend_context.cpp cg_fuji.cpp cg_fuji_control.cpp cg_fuji_data.cpp cg_fuji_scalar.cpp cg_fuji_avx512.cpp cg_fuji_vector.cpp cg_vector.cpp pass.cpp typing_pass.cpp flatten_statements_pass.cpp codegen_passes.cpp printing_pass.cpp propagate_predicates_pass.cpp restrictgen_pass.cpp compiler.cpp bench_tpch.cpp bench_tpch_rel.cpp safe_env.cpp clite.cpp explorer_helper.cpp benchmark_wait.cpp)

add_executable(voila main.cpp)
target_link_libraries(voila voila_compiler voila_runtime common rt)

add_executable(explorer explorer.cpp)
target_link_libraries(explorer voila_compiler voila_runtime common rt)



add_executable(test_buffering test_buffering.cpp test_buffering_helper.cpp)
target_link_libraries(test_buffering voila_runtime common)

add_executable(test_blend test_blend.cpp)

add_executable(test_ring_buffer test_ring_buffer.cpp)
enable_testing()

add_test(NAME test_tpch COMMAND ./test_tpch.py WORKING_DIRECTORY ${EXECUTABLE_OUTPUT_PATH})
