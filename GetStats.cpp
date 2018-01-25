#include<leveldb/db.h>
#include <leveldb/env.h>
#include<leveldb/filter_policy.h>
#include<iostream>
#include<string>
#include<leveldb/statistics.h>
using namespace std;
int main(int argc,char *argv[]){
  char *dbfilename = argv[1];
  leveldb::Options options;
  leveldb::DB *db;
  options.opEp_.stats_ = leveldb::CreateDBStatistics();
  options.opEp_.size_ratio = 10;
  leveldb::Status status = leveldb::DB::Open(options,dbfilename, &db);
  if(!status.ok()){
    fprintf(stderr,"can't open leveldb\n");
    cerr<<status.ToString()<<endl;
    exit(0);
  }
  string stat_str;
  db->GetProperty("leveldb.stats",&stat_str);
  cout<<stat_str<<endl;
  delete db;
  return 0;
}
