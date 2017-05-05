#ifndef YCSB_C_VLOGWB_DB_H
#define YCSB_C_VLOGWB_DB_H
#include "core/db.h"
#include <iostream>
#include <string>
#include "core/properties.h"
#include<VLogWriteBuffer.hh>
#include<configmod.hh>
using std::cout;
using std::endl;

namespace ycsbc {
class VLogDB : public DB{
public :
    VLogDB(const char *dbfilename,const char *configPath);
    int Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result);

  int Scan(const std::string &table, const std::string &key,
           int len, const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result) ;

  int Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values);

  int Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) ;

  int Delete(const std::string &table, const std::string &key);
    virtual ~VLogDB();

private:
    VLogWB *db_;
};
}
#endif