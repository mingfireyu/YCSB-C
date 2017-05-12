//
//  ycsbc.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/19/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include <cstring>
#include <string>
#include <iostream>
#include <vector>
#include <future>
#include "core/utils.h"
#include "core/timer.h"
#include "core/client.h"
#include "core/core_workload.h"
#include "db/db_factory.h"
#include<boost/timer.hpp>
using namespace std;

void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
string ParseCommandLine(int argc, const char *argv[], utils::Properties &props);

int DelegateClient(ycsbc::DB *db, ycsbc::CoreWorkload *wl, const int num_ops,
    bool is_loading) {
  db->Init();
  int ops[2] = {0,0};
  double durations[] = {0,0};
  ycsbc::Client client(*db, *wl);
  int oks = 0;
  for (int i = 0; i < num_ops; ++i) {
    if (is_loading) {
      oks += client.DoInsert();
    } else {
      oks += client.DoTransaction(ops,durations);
    }
    if(i%10000 == 0){
      cerr<<"operation count:"<<i<<"\r";
    }
  }
  cerr<<endl;
  if(!is_loading){
    cerr<<"WRITE latency"<<endl;
    cerr<<durations[ycsbc::Operation::INSERT]/ops[ycsbc::Operation::INSERT]<<"us"<<"Write ops:"<<ops[ycsbc::Operation::INSERT]<<endl;
    cerr<<"READ latency"<<endl;
    cerr<<durations[ycsbc::Operation::READ]/ops[ycsbc::Operation::READ]<<"us"<<"Read ops:"<<ops[ycsbc::Operation::READ]<<endl;;

    cout<<"WRITE latency"<<endl;
    cout<<durations[ycsbc::Operation::INSERT]/ops[ycsbc::Operation::INSERT]<<"us"<<"Write ops:"<<ops[ycsbc::Operation::INSERT]<<endl;
    cout<<"READ latency"<<endl;
    cout<<durations[ycsbc::Operation::READ]/ops[ycsbc::Operation::READ]<<"us"<<"Read ops:"<<ops[ycsbc::Operation::READ]<<endl;;

  }
  db->Close();
  return oks;
}

int main(const int argc, const char *argv[]) {
  utils::Properties props;
  string file_name = ParseCommandLine(argc, argv, props);

  ycsbc::DB *db = ycsbc::DBFactory::CreateDB(props);
  if (!db) {
    cout << "Unknown database name " << props["dbname"] << endl;
    exit(0);
  }

  ycsbc::CoreWorkload wl;
  wl.Init(props);

  const int num_threads = stoi(props.GetProperty("threadcount", "1"));

  // Loads data
  ycsbc::WallTimer loadRunTimer;
  loadRunTimer.Start();
  vector<future<int>> actual_ops;
  int total_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
  db->openStatistics();
  for (int i = 0; i < num_threads; ++i) {
    actual_ops.emplace_back(async(launch::async,
        DelegateClient, db, &wl, total_ops / num_threads, true));
  }
  assert((int)actual_ops.size() == num_threads);

  int sum = 0;
  for (auto &n : actual_ops) {
    assert(n.valid());
    sum += n.get();
  }
  cerr << "# Loading records:\t" << sum << endl;
  cerr << "load time: " << loadRunTimer.elapsed() <<"us"<<endl;
  cout << "# Loading records:\t" << sum << endl;
  cout << "load time: " << loadRunTimer.elapsed() <<"us"<<endl;

  //loadRunTimer.restart();
  // Peforms transactions
  actual_ops.clear();
  total_ops = stoi(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
  ycsbc::WallTimer timer;
  //  db->openStatistics();
  timer.Start();
  for (int i = 0; i < num_threads; ++i) {
    actual_ops.emplace_back(async(launch::async,
        DelegateClient, db, &wl, total_ops / num_threads, false));
  }
  assert((int)actual_ops.size() == num_threads);

  sum = 0;
  for (auto &n : actual_ops) {
    assert(n.valid());
    sum += n.get();
  }
  double duration = timer.elapsed();
  cerr<< endl;
  cerr << "# Transaction throughput (KTPS)" << endl;
  cerr << total_ops / (duration/1000000) / 1000 << endl;
  cerr << props["dbname"] << '\t' << file_name << '\t' << num_threads << '\t'<<endl;;
  cerr << "run time: " << timer.elapsed() <<"us"<<endl;
  cout << "# Transaction throughput (KTPS)" << endl;
  cout << total_ops / (duration/1000000) / 1000 << endl;
  cout << props["dbname"] << '\t' << file_name << '\t' << num_threads << '\t'<<endl;;
  cout << "run time: " << timer.elapsed() <<"us"<<endl;

  delete db;
}

string ParseCommandLine(int argc, const char *argv[], utils::Properties &props) {
  int argindex = 1;
  string filename;
  while (argindex < argc && StrStartWith(argv[argindex], "-")) {
    if (strcmp(argv[argindex], "-threads") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("threadcount", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-db") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("dbname", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-host") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("host", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-port") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("port", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-slaves") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("slaves", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-P") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      filename.assign(argv[argindex]);
      ifstream input(argv[argindex]);
      try {
        props.Load(input);
      } catch (const string &message) {
        cout << message << endl;
        exit(0);
      }
      input.close();
      argindex++;
    } else if(strcmp(argv[argindex],"-dbfilename") == 0){
	argindex++;
	if(argindex >= argc){
	    UsageMessage(argv[0]);
	    exit(0);
	}
	props.SetProperty("dbfilename", argv[argindex]);
        argindex++;
    }else if(strcmp(argv[argindex],"-configpath") == 0){
	argindex++;
	if(argindex >= argc){
	    UsageMessage(argv[0]);
	    exit(0);
	}
	props.SetProperty("configpath", argv[argindex]);
        argindex++;
    }else {
      cout << "Unknown option '" << argv[argindex] << "'" << endl;
      exit(0);
    }
  }

  if (argindex == 1 || argindex != argc) {
    UsageMessage(argv[0]);
    exit(0);
  }

  return filename;
}

void UsageMessage(const char *command) {
  cout << "Usage: " << command << " [options]" << endl;
  cout << "Options:" << endl;
  cout << "  -threads n: execute using n threads (default: 1)" << endl;
  cout << "  -db dbname: specify the name of the DB to use (default: basic)" << endl;
  cout << "  -P propertyfile: load properties from the given file. Multiple files can" << endl;
  cout << "                   be specified, and will be processed in the order specified" << endl;
}

inline bool StrStartWith(const char *str, const char *pre) {
  return strncmp(str, pre, strlen(pre)) == 0;
}

