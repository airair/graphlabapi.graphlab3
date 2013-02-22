/**  
 * Copyright (c) 2009 Carnegie Mellon University. 
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://www.graphlab.ml.cmu.edu
 *
 */


#include <graphlab/util/net_util.hpp>
#include <graphlab/util/mpi_tools.hpp>

namespace graphlab {
  namespace mpi_tools {
    /**
     * The init function is used to initialize MPI and must be called
     * to clean the command line arguments.
     */
    int init(int& argc, char**& argv, int required) {
#ifdef HAS_MPI
      static int provided(-1);
      if (initialized() == false) {
        int error = MPI_Init_thread(&argc, &argv, required, &provided);
        assert(error == MPI_SUCCESS);
      }
      return provided;
#else
      logstream(LOG_EMPH) << "MPI Support was not compiled." << std::endl;
      return -1;
#endif
    } 

    void finalize() {
      static bool finalized = false;
      if (finalized) return;
#ifdef HAS_MPI
      int error = MPI_Finalize();
      assert(error == MPI_SUCCESS);
      finalized = true;
#endif
    } 


    bool initialized() {
#ifdef HAS_MPI
      int ret_value = 0;
      int error = MPI_Initialized(&ret_value);
      assert(error == MPI_SUCCESS);
      return ret_value;
#else
      return false;
#endif
    } 

    size_t rank() {
#ifdef HAS_MPI
      int mpi_rank(-1);
      MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
      assert(mpi_rank >= 0);
      return size_t(mpi_rank);
#else
      return 0;
#endif
    }

    size_t size() {
#ifdef HAS_MPI
      int mpi_size(-1);
      MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
      assert(mpi_size >= 0);
      return size_t(mpi_size);
#else
      return 1;
#endif
    }

    void get_master_ranks(std::set<size_t>& master_ranks) {
      uint32_t local_ip = get_local_ip();
      std::vector<uint32_t> all_ips;
      all_gather(local_ip, all_ips);
      std::set<uint32_t> visited_ips;
      master_ranks.clear();
      for(size_t i = 0; i < all_ips.size(); ++i) {
        if(visited_ips.count(all_ips[i]) == 0) {
          visited_ips.insert(all_ips[i]);
          master_ranks.insert(i);
        }
      }
    }
  } 
}

