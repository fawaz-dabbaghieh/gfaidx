if(NOT DEFINED GFAIDX_BUILD_DIR OR NOT DEFINED GFAIDX_SOURCE_DIR)
    message(FATAL_ERROR "Build and source directories are required")
endif()

# Install into a test-only prefix, then configure and build a completely
# separate project using only the exported gfaidx package.
set(prefix "${GFAIDX_BUILD_DIR}/install-test-prefix")
set(consumer_build "${GFAIDX_BUILD_DIR}/install-test-consumer")
execute_process(
    COMMAND "${CMAKE_COMMAND}" --install "${GFAIDX_BUILD_DIR}" --prefix "${prefix}"
    RESULT_VARIABLE install_result)
if(NOT install_result EQUAL 0)
    message(FATAL_ERROR "gfaidx test installation failed")
endif()
execute_process(
    COMMAND "${CMAKE_COMMAND}" -S "${GFAIDX_SOURCE_DIR}/tests/consumer"
            -B "${consumer_build}" "-DCMAKE_PREFIX_PATH=${prefix}"
    RESULT_VARIABLE configure_result)
if(NOT configure_result EQUAL 0)
    message(FATAL_ERROR "External gfaidx consumer configuration failed")
endif()
execute_process(
    COMMAND "${CMAKE_COMMAND}" --build "${consumer_build}"
    RESULT_VARIABLE build_result)
if(NOT build_result EQUAL 0)
    message(FATAL_ERROR "External gfaidx consumer build failed")
endif()
execute_process(
    COMMAND "${consumer_build}/gfaidx_consumer"
    RESULT_VARIABLE run_result)
if(NOT run_result EQUAL 0)
    message(FATAL_ERROR "External gfaidx consumer failed")
endif()
