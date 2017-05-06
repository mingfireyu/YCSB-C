//
//  client.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/10/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include <string>
#include <boost/timer.hpp>
#include <boost/smart_ptr.hpp>
#include "db.h"
#include "core_workload.h"
#include "utils.h"
#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>
namespace ycsbc {

class Client {
 public:
  Client(DB &db, CoreWorkload &wl) : db_(db), workload_(wl) { 
	if(wl.with_timestamp_){
	    timestamp_trace_fp_ = wl.timestamp_trace_fp_;
	}else{
	    timestamp_trace_fp_ = NULL;
	}
	if(wl.with_operation_){
	  printf("operation from trace!\n");
	    current_operation_ = boost::make_shared<ycsbc::Operation>(ycsbc::Operation::READ);
	}else{
	    current_operation_ = nullptr;
	}
	line_ = NULL;
	first_do_transaction = false;
  }
  
  virtual bool DoInsert();
  virtual bool DoTransaction(int ops[],double durations[]);
  
  virtual ~Client() { }
  
 protected:
  
  virtual int TransactionRead();
  virtual int TransactionReadModifyWrite();
  virtual int TransactionScan();
  virtual int TransactionUpdate();
  virtual int TransactionInsert();
  void getCurrentTimeStamp();
  DB &db_;
  CoreWorkload &workload_;
  char *line_;
  boost::shared_ptr<ycsbc::Operation> current_operation_;
  FILE *timestamp_trace_fp_;
  boost::timer timer_;
  bool first_do_transaction;
  double current_time_;
  double current_timestamp_;
  
};

inline bool Client::DoInsert() {
  std::string key = workload_.NextSequenceKey();
  std::vector<DB::KVPair> pairs;
  workload_.BuildValues(pairs);
  return (db_.Insert(workload_.NextTable(), key, pairs) == DB::kOK);
}

inline bool Client::DoTransaction(int ops[],double durations[]) {
  int status = -1;
  
  if(!first_do_transaction){
	timer_.restart();
	first_do_transaction = true;
  }
  if(timestamp_trace_fp_){
	 current_time_ = timer_.elapsed()*1000000;
	 getCurrentTimeStamp();
	double delay =  current_timestamp_ - current_time_;
	//printf("current_time_:%.2lf current_timestamp_:%.2lf \n",current_time_,current_timestamp_);
	if(delay > 0){
	    usleep((unsigned int)delay);
	}
  }
  boost::timer transaction_timer;
  ycsbc::Operation operations = current_operation_ == nullptr ?workload_.NextOperation():*current_operation_;
  switch (operations) {
    case READ:
      status = TransactionRead();
      durations[READ] += (transaction_timer.elapsed()*1000000);
      ops[READ]++;
      break;
    case UPDATE:
      status = TransactionUpdate();
      durations[INSERT] += (transaction_timer.elapsed()*1000000);
      ops[INSERT]++;
      break;
    case INSERT:
      status = TransactionInsert();
      durations[INSERT] += (transaction_timer.elapsed()*1000000);
      ops[INSERT]++;
      break;
    case SCAN:
      status = TransactionScan();
      break;
    case READMODIFYWRITE:
      status = TransactionReadModifyWrite();
      break;
    default:
      throw utils::Exception("Operation request is not recognized!");
  }
  assert(status >= 0);
  return (status == DB::kOK);
}

inline int Client::TransactionRead() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> result;
  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    return db_.Read(table, key, &fields, result);
  } else {
    return db_.Read(table, key, NULL, result);
  }
}

inline int Client::TransactionReadModifyWrite() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> result;

  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    db_.Read(table, key, &fields, result);
  } else {
    db_.Read(table, key, NULL, result);
  }

  std::vector<DB::KVPair> values;
  if (workload_.write_all_fields()) {
    workload_.BuildValues(values);
  } else {
    workload_.BuildUpdate(values);
  }
  return db_.Update(table, key, values);
}

inline int Client::TransactionScan() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  int len = workload_.NextScanLength();
  std::vector<std::vector<DB::KVPair>> result;
  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    return db_.Scan(table, key, len, &fields, result);
  } else {
    return db_.Scan(table, key, len, NULL, result);
  }
}

inline int Client::TransactionUpdate() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> values;
  if (workload_.write_all_fields()) {
    workload_.BuildValues(values);
  } else {
    workload_.BuildUpdate(values);
  }
  return db_.Update(table, key, values);
}

inline int Client::TransactionInsert() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextSequenceKey();
  std::vector<DB::KVPair> values;
  workload_.BuildValues(values);
  return db_.Insert(table, key, values);
} 

inline void Client::getCurrentTimeStamp(){
	size_t len = 0;
	char operation;
	getline(&line_,&len,timestamp_trace_fp_);
	if(current_operation_){
	    sscanf(line_,"%lf,%c",&current_timestamp_,&operation);
	    *current_operation_ = operation == 'w' ? ycsbc::Operation::UPDATE:ycsbc::Operation::READ;
	}else{
	    sscanf(line_,"%lf,",&current_timestamp_);
	}
}

} // ycsbc

#endif // YCSB_C_CLIENT_H_
