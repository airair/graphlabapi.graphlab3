project(GraphLab)

add_library(graphlab3 STATIC
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
            database/graph_database_server.cpp
            database/distributed_graph/distributed_graph.cpp
            comm/comm_base.cpp
            comm/mpi_comm.cpp
            comm/mpi_comm2.cpp
            comm/comm_rpc.cpp
            comm/tcp_comm.cpp
            comm/tcp/dc_buffered_stream_send2.cpp
            comm/tcp/dc_stream_receive.cpp
            comm/tcp/dc_tcp_comm.cpp
            parallel/qthread_tools.cpp
           )

requires_core_deps(graphlab3)
