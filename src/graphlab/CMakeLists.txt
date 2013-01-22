project(GraphLab)

add_library(graphlab3 STATIC
            zookeeper/server_list.cpp
            util/event_log.cpp
            util/fs_util.cpp
            util/hdfs.cpp
            util/memory_info.cpp
            util/mpi_tools.cpp
            util/net_util.cpp
            util/random.cpp
            util/timer.cpp
            util/tracepoint.cpp
            util/circular_char_buffer.cpp
            util/web_util.cpp 
            parallel/pthread_tools.cpp 
            parallel/thread_pool.cpp
            logger/assertions.cpp 
            logger/logger.cpp
            database/graph_row.cpp
            database/graph_value.cpp
            comm/comm_base.cpp
            comm/mpi_comm.cpp
           )

requires_core_deps(graphlab3)
