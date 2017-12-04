#include "leveldb_db.h"
#include <cstring>
#include"basic_config.hh"
#include<iostream>
using namespace std;

namespace ycsbc {
LevelDB::LevelDB(const char* dbfilename,const char* configPath)
{
    leveldb::Options options;
    LevelDB_ConfigMod::getInstance().setConfigPath(configPath);
    std::string bloom_filename;
    char *bloom_filename_char;
    int bloom_bits;
    int max_open_files = LevelDB_ConfigMod::getInstance().getMax_open_files();
    int max_File_sizes = LevelDB_ConfigMod::getInstance().getMax_file_size();
    bool hierarchical_bloom_flag = LevelDB_ConfigMod::getInstance().getHierarchical_bloom_flag();
    bool log_open = LevelDB_ConfigMod::getInstance().getOpen_log();
    options.log_open = log_open;
    bool compression_Open = LevelDB_ConfigMod::getInstance().getCompression_flag();
    bool directIO_flag = LevelDB_ConfigMod::getInstance().getDirectIOFlag();
    int size_ratio = LevelDB_ConfigMod::getInstance().getSizeRatio();
    //    leveldb::setDirectIOFlag(directIO_flag);
    if(hierarchical_bloom_flag){
	bloom_filename = LevelDB_ConfigMod::getInstance().getBloom_filename();
	bloom_filename_char = (char *)malloc(sizeof(char)*bloom_filename.size()+1);
	strncpy(bloom_filename_char,bloom_filename.c_str(),bloom_filename.size());
	bloom_filename_char[bloom_filename.size()] = 0;
    }else{
	bloom_bits = LevelDB_ConfigMod::getInstance().getBloom_bits();
	printf("bloom_bits from config:%d\n",bloom_bits);
    }
    
    options.create_if_missing = true;
    options.compression = compression_Open?leveldb::kSnappyCompression:leveldb::kNoCompression;  //compression is disabled.
    options.max_file_size = max_File_sizes;
    options.max_open_files = max_open_files;
    options.size_ratio = size_ratio;
    fprintf(stderr,"size ratio:%d \n",size_ratio);
    if(LevelDB_ConfigMod::getInstance().getStatisticsOpen()){
	options.stats_ = leveldb::CreateDBStatistics();
    }
    if(hierarchical_bloom_flag){
	void *bloom_filename_ptr = (void *)bloom_filename_char;
	options.filter_policy = leveldb::NewBloomFilterPolicy(bloom_filename_ptr);
    }else{
	void *bloom_bits_ptr = (void *)(&bloom_bits);
	options.filter_policy = leveldb::NewBloomFilterPolicy(bloom_bits_ptr);
    }
    leveldb::setDirectIOFlag(directIO_flag);
    leveldb::Status status = leveldb::DB::Open(options,dbfilename, &db_);
    if(!status.ok()){
	fprintf(stderr,"can't open leveldb\n");
	exit(0);
    }
}

int LevelDB::Read(const string& table, const string& key, const vector< string >* fields, vector< DB::KVPair >& result)
{
    std::string value;
    leveldb::Status s = db_->Get(leveldb::ReadOptions(), key, &value);
    if(s.IsNotFound()){
	// fprintf(stderr,"not found!\n");
	return DB::kErrorNoData;
    }
    if(!s.ok()){
	 fprintf(stderr,"read error\n");
	 exit(0);
    }
    return DB::kOK;
}

int LevelDB::Insert(const string& table, const string& key, vector< DB::KVPair >& values)
{
    leveldb::Status s;
    int count = 0;
    //cout<<key<<endl;
    for(KVPair &p : values){
	//cout<<p.second.length()<<endl;
	s = db_->Put(leveldb::WriteOptions(), key, p.second);
	count++;
	if(!s.ok()){
	   fprintf(stderr,"insert error!\n");
	   cout<<s.ToString()<<endl;
	   exit(0);
	}
    }
    if(count != 1){
	  fprintf(stderr,"insert error\n");
	  exit(0);
    }
    return DB::kOK;
}

int LevelDB::Delete(const string& table, const string& key)
{
    vector<DB::KVPair> values;
    return Insert(table,key,values);
}

int LevelDB::Scan(const string& table, const string& key, int len, const vector< string >* fields, vector< vector< DB::KVPair > >& result)
{
    fprintf(stderr,"not implement yet");
    return DB::kOK;
}

int LevelDB::Update(const string& table, const string& key, vector< DB::KVPair >& values)
{
    return Insert(table,key,values);
}

void LevelDB::Close()
{
    std::string stat_str;
    db_->GetProperty("leveldb.stats",&stat_str);
    cout<<stat_str<<endl;
}


LevelDB::~LevelDB()
{
    delete db_;
}




}
