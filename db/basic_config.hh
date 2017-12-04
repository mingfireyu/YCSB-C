#ifndef BASIC_CONFIG_HH_
#define BASIC_CONFIG_HH_

#include <string>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <boost/concept_check.hpp>
#include <boost/smart_ptr.hpp>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include<cstddef>
typedef  unsigned long long  ULL;

template<typename T>
class Basic_ConfigMod {
public:

    static T& getInstance() {
         // Guaranteed to be destroyed
        if(instance == NULL){
	    instance = boost::make_shared<T>();
	}
        // Instantiated on first use
        return *instance;
    }

    void setConfigPath (const char* key);
  
protected:
    Basic_ConfigMod() {}
    ~Basic_ConfigMod() {}
    static boost::shared_ptr<T> instance;
    boost::property_tree::ptree _pt;
    bool readBool (const char* key);
    int readInt (const char* key);
    double readFloat(const char* key);
    ULL readULL (const char* key);
    unsigned long readUL(const char *key);
    std::string readString (const char* key);
    size_t readSize_t(const char* key);
    
   
};
class LevelDB_ConfigMod:public Basic_ConfigMod<LevelDB_ConfigMod>{
private:
    friend class Basic_ConfigMod<LevelDB_ConfigMod>;
    std::string _bloom_filename;
    int _max_open_files;
    bool _hierarchical_bloom_flag;
    int _bloom_bits;
    bool _open_log;
    int _max_file_size;
    bool _compression_flag;
    bool _directIO_flag;
    bool _statistics_open;
    int _size_ratio;
public:
    std::string getBloom_filename();
    int getMax_open_files();
    bool getHierarchical_bloom_flag();
    int getBloom_bits();
    bool getOpen_log();
    int getMax_file_size();
    void setConfigPath(const char*key);
    bool getCompression_flag();
    bool getDirectIOFlag();
    bool getStatisticsOpen();
    int getSizeRatio();
};

#endif
