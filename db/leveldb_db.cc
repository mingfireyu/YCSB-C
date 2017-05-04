#include "leveldb_db.h"
#include <cstring>

using namespace std;

namespace ycsbc {
LevelDB::LevelDB(const char* dbfilename)
{
    leveldb::Options options;
    options.create_if_missing = true;
    options.compression = leveldb::kNoCompression;  //compression is disabled.
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
	 fprintf(stderr,"not found!\n");
	return DB::kOK;
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
	   fprintf(stderr,"insert error\n");
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

LevelDB::~LevelDB()
{
    delete db_;
}




}