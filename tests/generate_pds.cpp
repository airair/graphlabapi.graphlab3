#include <iostream>
#include <vector>
using namespace std;
bool test_seq(int a, int b, int c, int q, std::vector<int>& result) {
  std::vector<int> res;
  int pdslength = q*q + q + 1;
  res.resize(pdslength + 3);
  res[0] = 0; res[1] = 0; res[2] = 1;
  int ctr = 2;
  for (int i = 3; i < res.size(); ++i) {
    res[i] = a * res[i - 1] + b * res[i - 2] + c * res[i - 3];
    res[i] = res[i] % q;
    ctr += (res[i] == 0);
    if (i < pdslength && ctr > q + 1) return false;
  }
  if (res[pdslength] == 0 && res[pdslength + 1] == 0){ // && res[pdslength + 2] == 1) {
    // we are good to go
    // now find the 0s
    for (int i = 0; i < pdslength; ++i) {
      if (res[i] == 0) {
        result.push_back(i);
      }
    }
    if (result.size() != q + 1) {
      result.clear();
      return false;
    }
    return true;
  }
  else {
    return false;
  } 
}


std::vector<int> find_pds(int q) {
  std::vector<int> res;
  int p = q;
  int pdslength = p *p + p + 1;
  for (int a = 0; a < q; ++a) {
    for (int b = 0; b < q; ++b) {
      if (b == 0 && a == 0) continue;
      for (int c = 1; c < q; ++c) {
        if (test_seq(a,b,c,q,res)) {
          return res;
        }
      }
    }
  } 
}

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cout << argv[0] << " [a prime number]\n";
    std::cout << "The resultant PDS will have modulus p^2 + p + 1\n";
    return 0;
  }
  int p = atoi(argv[1]);
  std::cout << "p = " << p << "\n";
  std::cout << "modulus = " << p*p+p+1 << "\n";
  std::vector<int> res = find_pds(p);
  std::cout << "PDS length = " << res.size() << "\n";
  for (size_t i = 0;i < res.size(); ++i) {
    std::cout << res[i] << "\t";
  }
  std::cout << "\n";
  // verify pdsness
  int pdslength = p *p + p + 1;
  std::vector<int> count(pdslength, 0);
  for (int i = 0;i < res.size(); ++i) {
    for (int j = 0;j < res.size(); ++j) {
      if (i == j) continue;
      count[(res[i] - res[j] + pdslength) % pdslength]++;
    }
  }
  bool ispds = true;
  for (int i = 1;i < count.size(); ++i) {
    if (count[i] != 1) ispds = false;
  }
  if (ispds) std::cout << "PDS Verified\n";
  else std::cout << "Not PDS\n";
  return 0;
}
