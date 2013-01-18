project(GraphLab)

add_library(graphlab3
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
            util/web_util.cpp 
            parallel/pthread_tools.cpp 
            parallel/thread_pool.cpp
            logger/assertions.cpp 
            logger/logger.cpp
            database/graph_row.cpp
            database/graph_value.cpp
           )

requires_core_deps(graphlab3)
