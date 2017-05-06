#include "vlogwb_db.h"
#include <cstring>

using namespace std;

namespace ycsbc {
VLogDB::VLogDB(const char* dbfilename,const char * configPath)
{
     ConfigMod::getInstance().setConfigPath(configPath);
     size_t c = ConfigMod::getInstance().getBufferCapacity();
     size_t f = ConfigMod::getInstance().getFileSize();
     std::string logName = ConfigMod::getInstance().getFileName();
     db_= new VLogWB(logName.c_str(),dbfilename,f,c);
}

int VLogDB::Read(const string& table, const string& key, const vector< string >* fields, vector< DB::KVPair >& result)
{
     std::string value;
    if(db_->Get( key, &value)){
	return DB::kOK;
    }
    return DB::kErrorNoData;
}

int VLogDB::Insert(const string& table, const string& key, vector< DB::KVPair >& values)
{
      
    int count = 0;
    //cout<<key<<endl;
    for(KVPair &p : values){
	//cout<<p.second.length()<<endl;
	if(!db_->Put(key, p.second)){
	        fprintf(stderr,"insert error\n");
		exit(0);
	}
	count++;
    }
    if(count != 1){
	  fprintf(stderr,"insert error\n");
	  exit(0);
    }
    return DB::kOK;
}

int VLogDB::Delete(const string& table, const string& key)
{
    vector<DB::KVPair> values;
    return Insert(table,key,values);
}

int VLogDB::Scan(const string& table, const string& key, int len, const vector< string >* fields, vector< vector< DB::KVPair > >& result)
{
    fprintf(stderr,"not implement yet");
    return DB::kOK;
}

int VLogDB::Update(const string& table, const string& key, vector< DB::KVPair >& values)
{
    return Insert(table,key,values);
}

void VLogDB::openStatistics() {
  db_->openStatistics();
}
VLogDB::~VLogDB()
{
    printf("delete VLogDB\n");
    delete db_;
}

}
