project(GraphLab)

# We require the most recent version of cmake and automatically
# install the correct version when using the cmake lists
cmake_minimum_required(VERSION 2.8)

# Libraries linked via full path no longer produce linker search paths.
cmake_policy(SET CMP0003 NEW)
# Preprocessor definition values are now escaped automatically.
cmake_policy(SET CMP0005 NEW)

# Determine where additional GraphLab specific cmake modules are
# defined
set(CMAKE_MODULE_PATH ${GraphLab_SOURCE_DIR}/cmake)

# TODO: what is this used by
set(BUILD_SHARED_LIBS ON)

include(CheckLibraryExists) 
include(CheckFunctionExists) 
include(ExternalProject)
include(CheckCXXSourceCompiles)
# # Display information about cmake variables
# include(CMakePrintSystemInformation)

# set include path
include_directories(
  ${GraphLab_SOURCE_DIR}/src
  ${GraphLab_SOURCE_DIR}/deps/local/include)

# set link path
link_directories(${GraphLab_SOURCE_DIR}/deps/local/lib)


if(NO_OPENMP)
  set(OPENMP_C_FLAGS "")
  set(OPENMP_LIBRARIES "")
  add_definitions(-D__NO_OPENMP__)
else()
  set(OPENMP_C_FLAGS "-fopenmp")
  set(OPENMP_LIBRARIES "gomp")
endif()


link_libraries(pthread ${OPENMP_LIBRARIES})

set(CPP11_FLAGS "")
if(CPP11)
  message(STATUS "C++11 Enabled")
  set(CPP11_FLAGS "-std=c++11 -Wno-enum-compare -Wno-conversion-null")
endif()

# Shared compiler flags used by all builds (debug, profile, release)
set(COMPILER_FLAGS "-Wall -g ${CPP11_FLAGS} ${OPENMP_C_FLAGS}" CACHE STRING "common compiler options")

# Disable address space randomization for OSX lion and above
if (APPLE)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Xlinker -no_pie")
endif()

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-rpath,${GraphLab_SOURCE_DIR}/deps/local/lib")

# Set subdirectories
subdirs(src tests)
if(EXPERIMENTAL)
  if (IS_DIRECTORY ${GraphLab_SOURCE_DIR}/experimental)
    subdirs(experimental)
  endif()
  subdirs(extapis)
endif()

if (IS_DIRECTORY ${GraphLab_SOURCE_DIR}/apps)
  subdirs(apps)
endif()


## ============================================================================
## ============================================================================
# Build external dependencies

# libbz  =================================================================
ExternalProject_Add(libbz2
  PREFIX ${GraphLab_SOURCE_DIR}/deps/libbz2
  URL http://www.bzip.org/1.0.6/bzip2-1.0.6.tar.gz
  URL_MD5 00b516f4704d4a7cb50a1d97e6e8e15b
  INSTALL_DIR ${GraphLab_SOURCE_DIR}/deps/local
  CONFIGURE_COMMAND ""
  PATCH_COMMAND patch -N -p0 -i ${GraphLab_SOURCE_DIR}/patches/libbz2_fpic.patch || true 
  BUILD_IN_SOURCE 1
  BUILD_COMMAND make install PREFIX=<INSTALL_DIR>
  INSTALL_COMMAND "" )




# Build Boost =================================================================
# GraphLab relies heavily on the boost library for parsing program
# options, filesystem support, random number generation, and 
# unordered containers. 
ExternalProject_Add(boost
  PREFIX ${GraphLab_SOURCE_DIR}/deps/boost
  URL "http://sourceforge.net/projects/boost/files/boost/1.50.0/boost_1_50_0.tar.gz"
  URL_MD5 dbc07ab0254df3dda6300fd737b3f264 
  BUILD_IN_SOURCE 1
  CONFIGURE_COMMAND 
  ./bootstrap.sh 
  --with-libraries=filesystem
  --with-libraries=program_options
  --with-libraries=system
  --with-libraries=iostreams
  --with-libraries=date_time
  --with-libraries=random
  --prefix=<INSTALL_DIR>
  BUILD_COMMAND  
  C_INCLUDE_PATH=${GraphLab_SOURCE_DIR}/deps/local/include 
  CPLUS_INCLUDE_PATH=${GraphLab_SOURCE_DIR}/deps/local/include 
  LIBRARY_PATH=${GraphLab_SOURCE_DIR}/deps/local/lib
  ./b2 install link=static variant=release threading=multi runtime-link=static
  INSTALL_COMMAND ""
  INSTALL_DIR ${GraphLab_SOURCE_DIR}/deps/local )
set(BOOST_ROOT ${GraphLab_SOURCE_DIR}/deps/local )
set(BOOST_LIBS_DIR ${GraphLab_SOURCE_DIR}/deps/local/lib)
set(Boost_LIBRARIES 
  ${BOOST_LIBS_DIR}/libboost_filesystem.a 
  ${BOOST_LIBS_DIR}/libboost_program_options.a
  ${BOOST_LIBS_DIR}/libboost_system.a 
  ${BOOST_LIBS_DIR}/libboost_iostreams.a 
  ${BOOST_LIBS_DIR}/libboost_date_time.a)
add_dependencies(boost libbz2)
message(STATUS "Boost libs: " ${Boost_LIBRARIES})
# add_definitions(-DBOOST_DATE_TIME_POSIX_TIME_STD_CONFIG)
# add_definitions(-DBOOST_ALL_DYN_LINK)
# set(Boost_SHARED_LIBRARIES "")
foreach(blib ${Boost_LIBRARIES})
  message(STATUS "Boost libs: " ${blib})
  string(REGEX REPLACE "\\.a$" ${CMAKE_SHARED_LIBRARY_SUFFIX} bout ${blib})
  message(STATUS "Boost dyn libs: " ${bout})
  set(Boost_SHARED_LIBRARIES ${Boost_SHARED_LIBRARIES} ${bout})
endforeach()
message(STATUS "Boost Shared libs: " ${Boost_SHARED_LIBRARIES})


# TCMalloc  ===================================================================
# We use tcmalloc for improved memory allocation performance
ExternalProject_Add(libtcmalloc
  PREFIX ${GraphLab_SOURCE_DIR}/deps/tcmalloc
  URL http://gperftools.googlecode.com/files/gperftools-2.0.tar.gz
  URL_MD5 13f6e8961bc6a26749783137995786b6
  CONFIGURE_COMMAND <SOURCE_DIR>/configure --enable-frame-pointers --prefix=<INSTALL_DIR> --enable-shared=no
  INSTALL_DIR ${GraphLab_SOURCE_DIR}/deps/local)
# link_libraries(tcmalloc)
set(TCMALLOC-FOUND 1)
add_definitions(-DHAS_TCMALLOC)







# zlib  =======================================================================
# We rely on the zlib library to read gzip compressed files
# (using boost iostreams).
# check_library_exists(z zlibVersion "" ZLIB_FOUND)
# if (ZLIB_FOUND)
#   add_definitions(-DHAS_ZLIB)
#   link_libraries(z)
# endif()

# libevent ====================================================================
# LibEvent is used in the RPC layer to manage the interaction between the 
# TCP stack and the event handler threads
ExternalProject_Add(libevent
  PREFIX ${GraphLab_SOURCE_DIR}/deps/event
  URL http://iweb.dl.sourceforge.net/project/levent/libevent/libevent-2.0/libevent-2.0.18-stable.tar.gz
  URL_MD5 aa1ce9bc0dee7b8084f6855765f2c86a
  CONFIGURE_COMMAND <SOURCE_DIR>/configure --prefix=<INSTALL_DIR> --disable-openssl
  INSTALL_DIR ${GraphLab_SOURCE_DIR}/deps/local)
#link_libraries(event)
#link_libraries(event_pthreads)


# JNI =========================================================================
# We use JNI for jvm interfacing and for hdfs support.  We currently only 
# detect the presence of java and do not install our own version. 

if(NOT NO_JAVAC)
  find_package(Java)
  find_package(JNI)


  if((EXISTS ${JAVA_INCLUDE_PATH}/jni.h))
    set(JNI_REALLY_FOUND 1 CACHE BOOL "Tests whether the header actually exists")
    message(STATUS "jni.h was found at " ${JAVA_INCLUDE_PATH}/jni.h)
    include_directories(${JNI_INCLUDE_DIRS})
    ## Guess java home location   
    if(EXISTS $ENV{JAVA_HOME} )
      set(JAVA_HOME $ENV{JAVA_HOME} CACHE STRING "Location of Java Home")
      message(STATUS "Java home set by user: " ${JAVA_HOME})
    else()
      string(REGEX REPLACE "\\/include" "" JAVA_HOME_GUESS  ${JAVA_AWT_INCLUDE_PATH})
      set(JAVA_HOME ${JAVA_HOME_GUESS} CACHE STRING "Location of Java Home")
      message(STATUS "Java Home guessed: " ${JAVA_HOME})
    endif()
  else( )
    set(JNI_REALLY_FOUND 0 CACHE BOOL "Tests whether the header actually exists")
    message(STATUS "jni.h was not found at " ${JAVA_INCLUDE_PATH}/jni.h)
  endif( )

  # Ant =========================================================================
  # Ant is used to build the graphlab JVM interface
  find_package(Ant)

  # LibHDFS ===================================================================== 
  # If JNI is found we install libhdfs which allows programs to read and write
  # to hdfs filesystems
  if( JNI_REALLY_FOUND )
    message(STATUS "Building libhdfs")
    ExternalProject_Add(hadoop
      PREFIX ${GraphLab_SOURCE_DIR}/deps/hadoop
      URL http://graphlab.org/deps/hadoop-1.0.1.tar.gz
#      URL http://www.gtlib.gatech.edu/pub/apache/hadoop/common/hadoop-1.0.1/hadoop-1.0.1.tar.gz
      URL_MD5 e627d9b688c4de03cba8313bd0bba148
      UPDATE_COMMAND chmod +x <SOURCE_DIR>/src/c++/libhdfs/install-sh <SOURCE_DIR>/src/c++/libhdfs/configure
      PATCH_COMMAND patch -N -p1 -i ${GraphLab_SOURCE_DIR}/patches/libhdfs.patch || true
      BUILD_IN_SOURCE 1
      CONFIGURE_COMMAND <SOURCE_DIR>/src/c++/libhdfs/configure JVM_ARCH=tune=generic --prefix=<INSTALL_DIR> --with-java=${JAVA_HOME} --enable-shared=no --enable-static=yes
      INSTALL_DIR ${GraphLab_SOURCE_DIR}/deps/local
      TEST_COMMAND bash -c "mkdir -p <INSTALL_DIR>/include && cp <SOURCE_DIR>/src/c++/libhdfs/hdfs.h <INSTALL_DIR>/include")
    set(HADOOP_FOUND 1 CACHE BOOL "Hadoop was found")
    add_definitions(-DHAS_HADOOP)
    # link_libraries(hdfs ${JAVA_JVM_LIBRARY})
  endif( JNI_REALLY_FOUND )
endif()

# zookeeper ================================================================
# Ant needed
find_package(Ant)

ExternalProject_Add(zookeeper
  PREFIX ${GraphLab_SOURCE_DIR}/deps/zookeeper
  URL http://apache.cs.utah.edu/zookeeper/zookeeper-3.4.5/zookeeper-3.4.5.tar.gz
  URL_MD5 f64fef86c0bf2e5e0484d19425b22dcb
  PATCH_COMMAND ${CMAKE_COMMAND} -E copy_directory ${GraphLab_SOURCE_DIR}/patches/zookeeper/ <SOURCE_DIR>
  BUILD_IN_SOURCE 1
  CONFIGURE_COMMAND ./configure --prefix=<INSTALL_DIR> --disable-shared
  INSTALL_DIR ${GraphLab_SOURCE_DIR}/deps/local)




# MPI =========================================================================
# MPI is currently used to establish communication between rpc nodes
if(NOT NO_MPI) 
find_package(MPICH2)
endif()

if(MPI_FOUND)
  message(STATUS "MPI Found: " ${MPI_LIBRARY} ${MPI_EXTRA_LIBRARY})
  add_definitions(-DHAS_MPI)
  include_directories(${MPI_INCLUDE_PATH})
else(MPI_FOUND)
  message(STATUS "MPI Not Found! Distributed Executables will not be compiled")
  set(MPI_LIBRARY "")
  set(MPI_EXTRA_LIBRARY "")
  # ExternalProject_Add(openmpi
  #   PREFIX ${GraphLab_SOURCE_DIR}/deps/openmpi
  #   URL http://www.open-mpi.org/software/ompi/v1.4/downloads/openmpi-1.4.5.tar.gz
  #   CONFIGURE_COMMAND <SOURCE_DIR>/configure --prefix=<INSTALL_DIR>
  #   INSTALL_DIR ${GraphLab_SOURCE_DIR}/deps/local)
endif(MPI_FOUND)




## =============================================================
# Core graphlab requirements 
macro(requires_core_deps NAME)
  target_link_libraries(${NAME} 
    ${Boost_LIBRARIES}
    zookeeper_mt
    z
    tcmalloc
    event event_pthreads)
  add_dependencies(${NAME} boost libtcmalloc libevent zookeeper)
  if(MPI_FOUND)
    target_link_libraries(${NAME} ${MPI_LIBRARY} ${MPI_EXTRA_LIBRARY})
  endif(MPI_FOUND)
  if(HADOOP_FOUND)
    target_link_libraries(${NAME} hdfs ${JAVA_JVM_LIBRARY})
    add_dependencies(${NAME} hadoop)
  endif(HADOOP_FOUND)
endmacro(requires_core_deps)
















## ============================================================================
## ============================================================================
## ============================================================================
# System Checks

## ============================================================================
# Test for cpu affinity support
set(CMAKE_REQUIRED_LIBRARIES "pthread")
check_function_exists(pthread_setaffinity_np HAS_SET_AFFINITY) 
set(CMAKE_REQUIRED_LIBRARIES ${crlbackup})  

include(CheckCXXCompilerFlag)
## ============================================================================
# check if MARCH is set
if (NOT MARCH)
  #set to native if supported
  check_cxx_compiler_flag(-march=native HAS_MARCH_NATIVE)
  if(HAS_MARCH_NATIVE)
    set(MARCH "native")
  else()
    set(MARCH "x86-64")
  endif()
endif()

# check if MTUNE is set
if (NOT MTUNE)
  #set to native if supported
  check_cxx_compiler_flag(-mtune=native HAS_MTUNE_NATIVE)
  if(HAS_MTUNE_NATIVE)
    set(MTUNE "native")
  else()
    set(MTUNE "x86-64")
  endif()
endif()

## ============================================================================
# Setup compiler

# If profiling mode is enabled then additional profiling flags are set for
# the compiler
if (COMPILE_PROFILING MATCHES 1) 
  set(PROFILING_FLAGS "-DUSE_EVENT_LOG -DUSE_TRACEPOINT")
else()
  set(PROFILING_FLAGS "")
endif()

# check for SSE instruction set (needed for CRC32)
#check_cxx_compiler_flag(-msse4.2 HAS_SSE42)
#set(SSE42_FLAG "")
#if (HAS_SSE42) 
# set(SSE42_FLAG "-msse4.2")
#endif()



# Set the debug flags
set(CMAKE_C_FLAGS_DEBUG 
  "-O0 -march=${MARCH} -Winit-self ${PROFILING_FLAGS} ${COMPILER_FLAGS}"
  CACHE STRING "compiler options" FORCE) 
set(CMAKE_CXX_FLAGS_DEBUG 
  "-O0 -march=${MARCH} -Winit-self ${PROFILING_FLAGS} ${COMPILER_FLAGS}"
  CACHE STRING "compiler options" FORCE) 

set(CMAKE_C_FLAGS_RELEASE 
  "-O3 -march=${MARCH} -mtune=${MTUNE} ${PROFILING_FLAGS} ${COMPILER_FLAGS}"
  CACHE STRING "compiler options" FORCE)
set(CMAKE_CXX_FLAGS_RELEASE 
  "-O3 -march=${MARCH} -mtune=${MTUNE} ${PROFILING_FLAGS} ${COMPILER_FLAGS}"
  CACHE STRING "compiler options" FORCE)

set(CMAKE_REQUIRED_FLAGS ${CMAKE_CXX_FLAGS_RELEASE}) 
check_cxx_source_compiles("int main(int argc, char** argv) { __builtin_ia32_crc32di(0,0);}" HAS_CRC32)
set(CMAKE_REQUIRED_FLAGS "")
if(HAS_CRC32) 
  set(CMAKE_CXX_FLAGS_RELEASE 
      "${CMAKE_CXX_FLAGS_RELEASE} -DHAS_BUILTIN_CRC32"
      CACHE STRING "compiler options" FORCE)
endif()


# # Mex setup
# set(CMAKE_CXX_FLAGS_MEX  
#   "${CMAKE_CXX_FLAGS_RELEASE} -D_GNU_SOURCE -fPIC -fno-omit-frame-pointer -pthread ")
# set(CMAKE_C_FLAGS_MEX  
#   "${CMAKE_C_FLAGS_RELEASE}   -D_GNU_SOURCE  -fexceptions -fPIC -fno-omit-frame-pointer -pthread ")


if (CMAKE_BUILD_TYPE MATCHES "Release")
  message(STATUS "Release build with C++ flags: " ${CMAKE_CXX_FLAGS_RELEASE})
  message(STATUS "Release build with C flags: " ${CMAKE_C_FLAGS_RELEASE})
elseif(CMAKE_BUILD_TYPE MATCHES "Debug")
  message(STATUS "Debug build with C++ flags: " ${CMAKE_CXX_FLAGS_DEBUG})
  message(STATUS "Debug build with C flags: " ${CMAKE_C_FLAGS_DEBUG})     
elseif(CMAKE_BUILD_TYPE MATCHES "Mex")
  message(STATUS "Mex CXX" ${CMAKE_CXX_FLAGS_MEX})
  message(STATUS "Mex C" ${CMAKE_C_FLAGS_MEX})
else()
  message(WARNING "Unknown build type: " ${CMAKE_BUILD_TYPE} "!")
endif()











## ============================================================================
## ============================================================================
## ============================================================================
# Build Macros

# copy_file ===================================================================
# copy a single file into build environment
macro(copy_file NAME)
  message(STATUS "Copying File: " ${NAME})
  file(INSTALL ${CMAKE_CURRENT_SOURCE_DIR}/${NAME} 
    DESTINATION   ${CMAKE_CURRENT_BINARY_DIR} )
endmacro(copy_file NAME)

# copy_files ==================================================================
# copy all files matching a pattern into the build environment
macro(copy_files NAME)
  message(STATUS "Copying Files: " ${NAME})
  file(INSTALL ${CMAKE_CURRENT_SOURCE_DIR}/  
    DESTINATION  ${CMAKE_CURRENT_BINARY_DIR} 
    FILES_MATCHING PATTERN ${NAME} )
endmacro(copy_files NAME)


# macro(EXEC file prog)
#   add_custom_command(
#     OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/${file}
#     COMMAND
#     ${prog}
#     DEPENDS ${prog}
#     WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
#     )
# endmacro(EXEC)

# add_graphlab_executable =====================================================
# copy all files matching a pattern into the build environment
macro(add_graphlab_executable NAME)
  add_executable(${NAME} ${ARGN})
  target_link_libraries(${NAME} graphlab3)
  add_dependencies(${NAME} graphlab3)
  requires_core_deps(${NAME})
endmacro(add_graphlab_executable)


add_custom_target(external_dependencies)
requires_core_deps(external_dependencies)







