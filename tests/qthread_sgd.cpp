#include <vector>
#include <iostream>
#include <stdint.h>
#include <cmath>
#include <cassert>
#include <cstdlib>
#include <boost/unordered_map.hpp>
#include <boost/bind.hpp>
#include <graphlab/serialization/serialization_includes.hpp>
#include <graphlab/comm/comm_base.hpp>
#include <graphlab/comm/mpi_comm.hpp>
#include <graphlab/util/mpi_tools.hpp>
#include <graphlab/util/random.hpp>
#include <graphlab/parallel/pthread_tools.hpp>
#include <graphlab/parallel/qthread_tools.hpp>
#include <graphlab/parallel/atomic.hpp>
#include <graphlab/parallel/qthread_future.hpp>
#include <graphlab/comm/comm_rpc.hpp>

#define WEIGHT_REQUEST (0)
#define WEIGHT_REPLY   (1)
#define WEIGHT_UPDATE  (2)
#define LOSS_INCREMENT (3)
#define WEIGHT_SYNC    (4)

std::vector<double> weights;
std::vector<double> true_weights;
double stepsize; // eta
size_t timestep;
size_t num_features_per_x;
size_t datapoints_so_far;

graphlab::comm_base* comm;
graphlab::comm_rpc* rpc;
graphlab::atomic<size_t> loss01;
graphlab::atomic<size_t> global_loss01;
graphlab::atomic<double> lossl2;
graphlab::atomic<double> global_lossl2;
graphlab::atomic<size_t> loss_count;
graphlab::atomic<size_t> weight_sync;
struct feature: public graphlab::IS_POD_TYPE {
  size_t id;
  double value;
  feature():id(0),value(0) { }
  feature(size_t id, double value): id(id), value(value) { }
};



struct request_future_result {
  graphlab::atomic<unsigned short> num_requests;
  boost::unordered_map<size_t, double> store;
  graphlab::mutex lock;
};


struct request_message {
  std::vector<size_t> ids;
  size_t request_handle_ptr;
  void save(graphlab::oarchive& oarc) const {
    oarc << ids << request_handle_ptr;
  }
  
  void load(graphlab::iarchive& iarc) {
    iarc >> ids >> request_handle_ptr;
  }
};


struct reply_message {
  std::vector<feature> res;
  size_t request_handle_ptr;
  void save(graphlab::oarchive& oarc) const {
    oarc << res << request_handle_ptr;
  }
  
  void load(graphlab::iarchive& iarc) {
    iarc >> res >> request_handle_ptr;
  }
};


struct update_future_result {
  graphlab::atomic<unsigned short> num_requests;
};



struct update_message {
  std::vector<feature> res;
  void save(graphlab::oarchive& oarc) const {
    oarc << res;
  }
  
  void load(graphlab::iarchive& iarc) {
    iarc >> res;
  }
};

struct update_reply_message {
  size_t update_handle_ptr;
  void save(graphlab::oarchive& oarc) const {
    oarc << update_handle_ptr;
  }
  
  void load(graphlab::iarchive& iarc) {
    iarc >> update_handle_ptr;
  }
};



/**
 * Sends out a request for all weights required for a data point
 */
void send_requests(const std::vector<feature>& x, 
                   request_future_result* result) {
  std::vector<request_message> message;
  message.resize(comm->size());
  for (size_t i = 0; i < x.size(); ++i) {
    size_t targetmachine = x[i].id % comm->size();
    if (targetmachine == comm->rank()) result->store[x[i].id] = weights[x[i].id];
    else message[targetmachine].ids.push_back(x[i].id);
  }
  // figure out the number of requests we are making
  size_t numrequests = 0;
  for (size_t i = 0;i < comm->size(); ++i) numrequests += (message[i].ids.size() > 0);
  result->num_requests.value = numrequests;
  if (numrequests == 0) {
    graphlab::qthread_future<request_future_result>::signal(result);
    return;
  }
  //printf("Req 0x%lx\n", result);
  // fill in the request_handle_pointer in the message
  // and send it out
  for (size_t i = 0;i < comm->size(); ++i) {
    if (message[i].ids.size() > 0) {
      message[i].request_handle_ptr = reinterpret_cast<size_t>(result);
      graphlab::oarchive* oarc = rpc->prepare_message(WEIGHT_REQUEST);
      (*oarc) << message[i];
      rpc->complete_message(i, oarc);
    }
  }
}



void process_request(graphlab::comm_rpc* rpc,
                     int source, const char* c, size_t len) {
  graphlab::iarchive iarc(c, len);
  request_message msg;
  iarc >> msg;
  
  // generate the reply object
  reply_message reply;
  for (size_t i = 0;i < msg.ids.size(); ++i) {
    reply.res.push_back(feature(msg.ids[i], weights[msg.ids[i]]));
  }

  // fill the request handle
  reply.request_handle_ptr = msg.request_handle_ptr;

  // serialize the reply
  graphlab::oarchive* oarc = rpc->prepare_message(WEIGHT_REPLY);
  (*oarc) << reply;
  rpc->complete_message(source, oarc);
}

void process_reply(graphlab::comm_rpc* comm,
                   int source, const char* c, size_t len) {
  graphlab::iarchive iarc(c, len);
  reply_message reply;
  iarc >> reply;

  // get the request pointer back
  request_future_result* req = 
      reinterpret_cast<request_future_result*>(reply.request_handle_ptr);

  //printf("Req Rep 0x%lx\n", req);
  req->lock.lock();
  for (size_t i = 0; i < reply.res.size(); ++i) {
    req->store[reply.res[i].id] = reply.res[i].value;
  }
  req->lock.unlock();
  if (req->num_requests.dec() == 0) {
    graphlab::qthread_future<request_future_result>::signal(req);
  }
}

void send_update(const boost::unordered_map<size_t, double>& updates) {
  std::vector<update_message> message;
  message.resize(comm->size());
  boost::unordered_map<size_t, double>::const_iterator iter = updates.begin();
  while (iter != updates.end()) {
    size_t targetmachine = iter->first % comm->size();
    if (targetmachine == comm->rank()) weights[iter->first] += iter->second;
    else message[targetmachine].res.push_back(feature(iter->first, iter->second));
    ++iter;
  }
  // fill in the request_handle_pointer in the message
  // and send it out
  for (size_t i = 0;i < comm->size(); ++i) {
    if (message[i].res.size() > 0) {

      graphlab::oarchive* oarc = rpc->prepare_message(WEIGHT_UPDATE);
      (*oarc) << message[i];
      rpc->complete_message(i, oarc);
    }
  }
}

void process_update(graphlab::comm_rpc* comm,
                    int source, const char* c, size_t len) {
  graphlab::iarchive iarc(c, len);
  update_message msg;
  iarc >> msg;

  for (size_t i = 0;i < msg.res.size(); ++i) {
    weights[msg.res[i].id] += msg.res[i].value;
  }
}

void process_loss_update(graphlab::comm_rpc* rpc,
                         int source, const char* c, size_t len) {
  graphlab::iarchive iarc(c, len);
  size_t loss; double dloss;
  iarc >> loss >> dloss;
  global_loss01.inc(loss);
  global_lossl2.inc(dloss);
  if (loss_count.value == rpc->get_comm()->size() - 1) {
    std::cout << "Average Loss01 = " 
              << global_loss01.value << " / " << datapoints_so_far << ": " 
              << (double)(global_loss01.value) / datapoints_so_far << std::endl;
    std::cout << "Average LossL2 = " << global_lossl2.value / datapoints_so_far << std::endl;
  }
  loss_count.inc();
}

void process_weight_sync(graphlab::comm_rpc* rpc,
                         int source, const char* c, size_t len) {
  graphlab::iarchive iarc(c, len);
  std::vector<double> other_weight;
  iarc >> other_weight;
  std::cout << "Receiving weights from " << source << "\n";
  for (size_t i = source; i < weights.size(); i += rpc->get_comm()->size()) {
    weights[i] = other_weight[i];
  }
  weight_sync.inc(); 
}

/**
 * Takes a logistic gradient step using the datapoint (x,y)
 * changes the global variable weights, timestep
 * Also returns the predicted value for the datapoint
 */
double logistic_sgd_step(const std::vector<feature>& x, double y) {
  // compute predicted value of y
  double linear_predictor = 0;
  graphlab::qthread_future<request_future_result> qfuture;
  send_requests(x, &qfuture.get());
  qfuture.wait();
  assert(qfuture.get().num_requests == 0);
  boost::unordered_map<size_t, double>& w = qfuture.get().store;
  for (size_t i = 0; i < x.size(); ++i) {
    linear_predictor += x[i].value * w[x[i].id];
  } 

  // probability that y is 0
  double py0 = 1.0 / (1 + std::exp(linear_predictor));
  double py1 = 1.0 - py0;
  // note that there is a change that we get NaNs here. If we get NaNs,
  // push down the step size 
  
  // ok compute the gradient
  // the gradient update is easy
  // now, we are going to only store the deltas.
  // thus the "=" instead of the +=
  for (size_t i = 0; i < x.size(); ++i) {
    w[x[i].id] = stepsize / (sqrt(1.0 + timestep)) * (double(y) - py1) * x[i].value; // atomic
  }

  send_update(w);
  return py1;
}


void generate_ground_truth_weight_vector(size_t numweights, int wseed = -1) {
  if (wseed != -1) graphlab::random::seed(wseed);
  // generate a random small weight vector between -1 and 1
  true_weights.resize(numweights);
  for (size_t i = 0; i < numweights; ++i) {
    true_weights[i] = graphlab::random::fast_uniform<double>(-1.0,1.0);
  }
}


/**
 * Generates a simple synthetic binary classification dataset in X,Y with numdata 
 * datapointsand where each datapoint has numweights weights and num_features_per_x features
 * Every weight value will be between -1 and 1.
 * Also returns the true weight vector used to generate the data.
 * (the dataset generated by this procedure is actually quite hard to learn)
 *
 * If wseed is not -1, a seed is used to generate the weights. after which
 * the generator will be reset with a non-det seed.
 */
void generate_datapoint(std::vector<feature> & x,
                        double& y,
                        size_t num_features_per_x) {
  x.clear();
  // use logistic regression to predict a y
  double linear_predictor = 0;
  for (size_t j = 0; j < num_features_per_x; ++j) {
    x.push_back(feature(graphlab::random::fast_uniform<size_t>(0, true_weights.size() - 1),
                        graphlab::random::fast_uniform<double>(-1.0,1.0)));
    linear_predictor += x[j].value * true_weights[x[j].id];
  }
  double py0 = 1.0 / (1 + std::exp(linear_predictor));
  double py1 = 1.0 - py0;
  // generate a 0/1Y value. 
  y = graphlab::random::rand01() >= py1;
  //if (graphlab::random::rand01() < 0.1) y = !y;
  //y = py1 + graphlab::random::gaussian();
}


double test_data_point(const std::vector<feature>& x, double Y,
                     const std::vector<double>& w) {
  double linear_predictor = 0;
  for (size_t j = 0; j < x.size(); ++j) {
    linear_predictor += x[j].value * weights[x[j].id];
  }
  double py0 = 1.0 / (1 + std::exp(linear_predictor));
  double py1 = 1.0 - py0;
  return py1;
}

void data_loop(size_t num_points) {
  std::vector<feature> x;
  double y;
  for (size_t i = 0; i < num_points; ++i) {
    generate_datapoint(x,y,num_features_per_x);
    double ret = logistic_sgd_step(x,y);
    lossl2.inc((y - ret) * (y - ret));
    loss01.inc(y != (ret >= 0.5));
  }
}

int main(int argc, char** argv) {
  size_t ndata = 1000000;
  size_t numthreads = 100;
  size_t numweights = 10000;
  num_features_per_x = 100;
  datapoints_so_far = 0;

  if (argc > 1) ndata = atoi(argv[1]);
  if (argc > 2) numthreads = atoi(argv[2]);
  if (argc > 3) numweights = atoi(argv[3]);
  if (argc > 4) num_features_per_x = atoi(argv[4]);
  std::cout << "Generating " << ndata << " datapoints\n";
  std::cout << "Using " << numthreads << " threads per machine\n";
  std::cout << numweights << " weights\n";
  std::cout << "Features Per X: " << num_features_per_x <<  "\n";
  stepsize = 0.01;



  if (argc > 5) {
    comm = graphlab::comm_base::create(argv[5], &argc, &argv);
  } else { 
    comm = graphlab::comm_base::create("mpi", &argc, &argv);
  }
  assert(comm != NULL);
  comm->barrier();

  rpc = new graphlab::comm_rpc(comm);
  // register the functions

  rpc->register_handler(WEIGHT_REQUEST, process_request);
  rpc->register_handler(WEIGHT_REPLY, process_reply);
  rpc->register_handler(WEIGHT_UPDATE, process_update); 
  rpc->register_handler(LOSS_INCREMENT, process_loss_update); 
  rpc->register_handler(WEIGHT_SYNC, process_weight_sync); 

  comm->barrier();
  // set the stacksize to 8192
  graphlab::qthread_tools::init(-1,12288);
  // generate ground truth vector
  generate_ground_truth_weight_vector(numweights, 1234);
  weights.resize(numweights);


  // generate test data
  std::vector<std::vector<feature> > testX(1000);
  std::vector<double> testY(1000);
  for (size_t i = 0;i < testY.size(); ++i) {
    generate_datapoint(testX[i], testY[i], num_features_per_x);
  }

  loss01 = 0;
  global_loss01 = 0;
  lossl2 = 0.0;
  global_lossl2 = 0.0;
  loss_count = 0;
  weight_sync = 0;
  comm->barrier();
  graphlab::qthread_group group;
  // we synchronize 100 times for ndata points
  size_t points_per_machine = ndata / comm->size();
  size_t points_per_iter = points_per_machine / 100;
  size_t points_per_thread = points_per_iter / numthreads;
  if (points_per_thread == 0) points_per_thread = 1;

  std::cout << "After dividing, effective number of data points = " 
            << points_per_thread * numthreads * comm->size() * 100 << "\n";
            
  comm->barrier();
  for (size_t iter = 0; iter < 100; ++iter) {
    datapoints_so_far = (iter + 1) * points_per_iter * comm->size();
    graphlab::timer ti; ti.start();
    timestep = iter;
    for (size_t i = 0;i < numthreads; ++i) {
      group.launch(boost::bind(data_loop, points_per_thread));
    }
    group.join();  
    comm->barrier();
    if (comm->rank() == 0) std::cout << ti.current_time() << std::endl;

    graphlab::oarchive* oarc = rpc->prepare_message(LOSS_INCREMENT);
    (*oarc) << loss01.value << lossl2.value;
    rpc->complete_message(0, oarc);

    if (comm->rank() == 0) {
      while(loss_count.value < comm->rank()) cpu_relax();
    }
  
    if (comm->rank() > 0) {
      graphlab::oarchive* oarc = rpc->prepare_message(WEIGHT_SYNC);
      (*oarc) << weights;
      rpc->complete_message(0, oarc);
    }
    if (comm->rank() == 0) {
      while(weight_sync.value + 1 < comm->rank()) cpu_relax();
  
      double testlossl2 = 0;
      size_t testloss01 = 0;
      for (size_t i = 0;i < testY.size(); ++i) {
        double ret = test_data_point(testX[i], testY[i], weights);
        testlossl2 += ((testY[i] - ret) * (testY[i] - ret));
        testloss01 += (testY[i] != (ret >= 0.5));
      }
      std::cout << "Test Loss 01: " << double(testloss01) / testY.size() << "\n";
      std::cout << "Test Loss L2: " << testlossl2 / testY.size() << "\n";
    }

    if (comm->rank() == 0) {
      std::cout << iter << " iterations\n";
    }
    loss_count = 0;
    weight_sync = 0;
    loss01 = 0;
    lossl2 = 0.0;

    comm->barrier();    
  }
  delete comm;
  delete rpc;
  graphlab::qthread_tools::finalize();
  graphlab::mpi_tools::finalize();
}
