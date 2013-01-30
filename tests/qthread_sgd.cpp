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
#include <graphlab/util/random.hpp>
#include <graphlab/parallel/pthread_tools.hpp>
#include <graphlab/parallel/qthread_tools.hpp>
#include <graphlab/parallel/atomic.hpp>
#include <graphlab/parallel/qthread_external_future.hpp>

#define WEIGHT_REQUEST (char(0))
#define WEIGHT_REPLY   (char(1))
#define WEIGHT_UPDATE (char(2))
#define WEIGHT_UPDATE_REPLY   (char(3))
#define LOSS_INCREMENT (char(4))

std::vector<double> weights;
double stepsize; // eta
size_t timestep;
graphlab::comm_base* comm;
graphlab::atomic<size_t> loss01;
graphlab::atomic<size_t> global_loss01;
graphlab::atomic<double> lossl2;
graphlab::atomic<double> global_lossl2;
graphlab::atomic<size_t> loss_count;
struct feature: public graphlab::IS_POD_TYPE {
  size_t id;
  double value;
  feature():id(0),value(0) { }
  feature(size_t id, double value): id(id), value(value) { }
};



struct request_future_result {
  graphlab::atomic<unsigned short> num_requests;
  size_t future_handle;
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
  size_t future_handle;
};



struct update_message {
  std::vector<feature> res;
  size_t update_handle_ptr;
  void save(graphlab::oarchive& oarc) const {
    oarc << res << update_handle_ptr;
  }
  
  void load(graphlab::iarchive& iarc) {
    iarc >> res >> update_handle_ptr;
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
                   size_t future_handle,
                   request_future_result* result) {
  std::vector<request_message> message;
  message.resize(comm->size());
  result->future_handle = future_handle;
  for (size_t i = 0; i < x.size(); ++i) {
    message[x[i].id % comm->size()].ids.push_back(x[i].id);
  }
  // figure out the number of requests we are making
  size_t numrequests = 0;
  for (size_t i = 0;i < comm->size(); ++i) numrequests += (message[i].ids.size() > 0);
  result->num_requests.value = numrequests;
  //printf("Req 0x%lx\n", result);
  // fill in the request_handle_pointer in the message
  // and send it out
  for (size_t i = 0;i < comm->size(); ++i) {
    if (message[i].ids.size() > 0) {
      message[i].request_handle_ptr = reinterpret_cast<size_t>(result);
      graphlab::oarchive oarc;
      oarc << WEIGHT_REQUEST << message[i];
      comm->send(i, oarc.buf, oarc.off);
      free(oarc.buf);
    }
  }
}



void process_request(graphlab::comm_base* comm,
                     size_t source, const char* c, size_t len) {
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
  graphlab::oarchive oarc;
  oarc << WEIGHT_REPLY << reply;
  comm->send(source, oarc.buf, oarc.off); 
  free(oarc.buf);
}

void process_reply(graphlab::comm_base* comm,
                     size_t source, const char* c, size_t len) {
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
    graphlab::qthread_external_future<request_future_result>::signal(req->future_handle);
  }
}

void send_update(const boost::unordered_map<size_t, double>& updates,
                   size_t future_handle,
                   update_future_result* result) {
  result->future_handle = future_handle;
  std::vector<update_message> message;
  message.resize(comm->size());
  boost::unordered_map<size_t, double>::const_iterator iter = updates.begin();
  while (iter != updates.end()) {
    message[iter->first % comm->size()].res.push_back(feature(iter->first, iter->second));
    ++iter;
  }
  
  size_t numrequests = 0;
  for (size_t i = 0;i < comm->size(); ++i) numrequests += (message[i].res.size() > 0);
  result->num_requests.value = numrequests;

  // fill in the request_handle_pointer in the message
  // and send it out
  for (size_t i = 0;i < comm->size(); ++i) {
    if (message[i].res.size() > 0) {
      message[i].update_handle_ptr = reinterpret_cast<size_t>(result);
      graphlab::oarchive oarc;
      oarc << WEIGHT_UPDATE << message[i];
      comm->send(i, oarc.buf, oarc.off);
      free(oarc.buf);
    }
  }
}

void process_update(graphlab::comm_base* comm,
                    size_t source, const char* c, size_t len) {
  graphlab::iarchive iarc(c, len);
  update_message msg;
  iarc >> msg;

  for (size_t i = 0;i < msg.res.size(); ++i) {
    weights[msg.res[i].id] += msg.res[i].value;
  }

  update_reply_message reply;
  // fill the request handle
  reply.update_handle_ptr = msg.update_handle_ptr;

  // serialize the reply
  graphlab::oarchive oarc;
  oarc << WEIGHT_UPDATE_REPLY << reply;
  comm->send(source, oarc.buf, oarc.off); 
  free(oarc.buf);
}

void process_update_reply(graphlab::comm_base* comm,
                          size_t source, const char* c, size_t len) {
  graphlab::iarchive iarc(c, len);
  update_reply_message reply;
  iarc >> reply;
  // get the request pointer back
  update_future_result* req = 
      reinterpret_cast<update_future_result*>(reply.update_handle_ptr);
  if (req->num_requests.dec() == 0) {
    graphlab::qthread_external_future<update_future_result>::signal(req->future_handle);
  }
}

void process_loss_update(graphlab::comm_base* comm,
                         size_t source, const char* c, size_t len) {
  graphlab::iarchive iarc(c, len);
  size_t loss; double dloss;
  iarc >> loss >> dloss;
  global_loss01.inc(loss);
  global_lossl2.inc(dloss);
  if (loss_count.value == comm->size() - 1) {
    std::cout << "Loss01 = " << global_loss01.value << std::endl;
    std::cout << "LossL2 = " << global_lossl2.value << std::endl;
  }
  loss_count.inc();
}

void receive_dispatch(int machine, char* c, size_t len) {
  assert(len > 0);
  char ptype = c[0];
  ++c;
  --len;
  switch(ptype) {
    case WEIGHT_REQUEST:
      //std::cout << "wreq\n";
      process_request(comm, (size_t)(machine), c, len);
      break;
    case WEIGHT_REPLY:
      //std::cout << "wreq_rep\n";
      process_reply(comm, (size_t)(machine), c, len);
      break;
    case WEIGHT_UPDATE:
      //std::cout << "wup\n";
      process_update(comm, (size_t)(machine), c, len);
      break;
    case WEIGHT_UPDATE_REPLY:
      //std::cout << "wup_rep\n";
      process_update_reply(comm, (size_t)(machine), c, len);
      break;
    case LOSS_INCREMENT:
      //std::cout << "wup_rep\n";
      process_loss_update(comm, (size_t)(machine), c, len);
      break;
    default:
      std::cerr << "Unexpected packet type: " << (int)(ptype) << std::endl;
      assert(false);
  }
}

/**
 * Takes a logistic gradient step using the datapoint (x,y)
 * changes the global variable weights, timestep
 * Also returns the predicted value for the datapoint
 */
double logistic_sgd_step(const std::vector<feature>& x, double y) {
  // compute predicted value of y
  double linear_predictor = 0;
  graphlab::qthread_external_future<request_future_result> qfuture;
  send_requests(x, qfuture.get_handle(), qfuture.get_result_ptr());
  qfuture.wait();
  assert(qfuture.get_result_ptr()->num_requests == 0);
  boost::unordered_map<size_t, double>& w = qfuture.get_result_ptr()->store;
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

  graphlab::qthread_external_future<update_future_result> qfuture_update;
  send_update(w, qfuture_update.get_handle(), qfuture_update.get_result_ptr());
  qfuture_update.wait();
  assert(qfuture_update.get_result_ptr()->num_requests == 0);  
  return py1;
}



/**
 * Generates a simple synthetic binary classification dataset in X,Y with numdata 
 * datapointsand where each datapoint has numweights weights and average 
 * sparsity "sparsity".
 * Every weight value will be between -1 and 1.
 * Also returns the true weight vector used to generate the data.
 * (the dataset generated by this procedure is actually quite hard to learn)
 *
 * If wseed is not -1, a seed is used to generate the weights. after which
 * the generator will be reset with a non-det seed.
 */
std::vector<double> generate_dataset(std::vector<std::vector<feature> >& X,
                                     std::vector<double>& Y,
                                     int numweights,
                                     int numdata,
                                     double sparsity,
                                     int wseed = -1) {
  assert(0 < sparsity  && sparsity <= 1.0);
  if (wseed != -1) graphlab::random::seed(wseed);
  // generate a random small weight vector between -1 and 1
  std::vector<double> w(numweights, 0);
  for (size_t i = 0; i < numweights; ++i) {
    w[i] = graphlab::random::fast_uniform<double>(-1.0,1.0);
  }
  std::cout << w[0] << "\n";

  if (wseed != -1) graphlab::random::nondet_seed();

  for (size_t i = 0; i < numdata; ++i) {
    std::vector<feature> x;  
    // use logistic regression to predict a y
    double linear_predictor = 0;
    // generate a random 25% sparse datapoint
    for (size_t j = 0; j < numweights; ++j) {
      // with 25T probability generate a weight
      if (graphlab::random::bernoulli(sparsity) == 0) {
        x.push_back(feature(j, 
                            graphlab::random::fast_uniform<double>(-1.0,1.0)));
        linear_predictor += x.rbegin()->value * w[i];
      }
    }
    double py0 = 1.0 / (1 + std::exp(linear_predictor));
    double py1 = 1.0 - py0;
    // generate a 0/1Y value. 
    //double yval = py1 >= 0.5;
    // to get regression, comment the line above and uncomment the line below
    double yval = py1;

    X.push_back(x);
    Y.push_back(yval);
  }
  return w;
}


void data_loop(std::vector<std::vector<feature> >* X,
               std::vector<double>* Y,
               size_t idx, size_t numthreads) {
  size_t xstart = X->size() * idx / numthreads;
  size_t xend = X->size() * (idx + 1) / numthreads;
  assert(xstart < xend);
  assert(xstart < X->size());
  assert(xend <= X->size());
  for (size_t i = xstart ; i < xend; ++i) {
    double ret = logistic_sgd_step((*X)[i], (*Y)[i]);
    lossl2.inc(((*Y)[i] - ret) * ((*Y)[i] - ret));
    loss01.inc((*Y)[i] != (ret >= 0.5));
  }
}

int main(int argc, char** argv) {
  stepsize = 0.1;
  size_t numweights = 100;
  weights.resize(numweights, 0.0); // actual weights are based on mod p

  // make a small send window
  comm = new graphlab::mpi_comm(&argc, &argv);
  comm->register_receiver(receive_dispatch, true);
  graphlab::qthread_tools::init(4, 8192);
  // generate a little test dataset
  std::vector<std::vector<feature> > X;
  std::vector<double> Y;
  std::vector<double> weights = generate_dataset(X, Y, numweights, 100000 / comm->size() , 0.1,
                                                 1234);
  std::cout << "Data generated\n";
  loss01 = 0;
  global_loss01 = 0;
  lossl2 = 0.0;
  global_lossl2 = 0.0;
  loss_count = 0;
  comm->barrier();
  graphlab::qthread_group group;
  size_t numthreads = 1000;
  for (size_t iter = 0; iter < 100; ++iter) {
    graphlab::timer ti; ti.start();
    timestep = iter;
    for (size_t i = 0;i < numthreads; ++i) {
      group.launch(boost::bind(data_loop, &X, &Y, i, numthreads));
    }
    group.join();  
    comm->barrier();
    if (comm->rank() == 0) std::cout << ti.current_time() << std::endl;
    graphlab::oarchive oarc;
    oarc << LOSS_INCREMENT << loss01.value << lossl2.value;
    comm->send(0, oarc.buf, oarc.off);
    free(oarc.buf);
    if (comm->rank() == 0) {
      while(loss_count.value < comm->rank()) cpu_relax();
    }
    loss01 = 0;
    loss_count = 0;
    global_loss01 = 0;
    lossl2 = 0.0;
    global_lossl2 = 0.0;
    if (comm->rank() == 0) {
      std::cout << iter << " iterations\n";
    }
    comm->barrier();    
  }
}
