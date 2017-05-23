#include"basic_config.hh"
#include<iostream>
template<typename T>
void Basic_ConfigMod<T>::setConfigPath (const char* path) {
    boost::property_tree::ini_parser::read_ini(path, _pt);
    assert (!_pt.empty());
}
template<typename T>
bool Basic_ConfigMod<T>::readBool (const char* key) {
    return _pt.get<bool>(key);
}
template<typename T>
int Basic_ConfigMod<T>::readInt (const char* key) {
    return _pt.get<int>(key);
}
template<typename T>
std::string Basic_ConfigMod<T>::readString (const char* key) {
    return _pt.get<std::string>(key);
}


void LevelDB_ConfigMod::setConfigPath(const char*path){
    boost::property_tree::ini_parser::read_ini(path, _pt);
    assert (!_pt.empty());
    _bloom_filename = readString("basic.bloomFilename");
    _bloom_bits = readInt("basic.bloomBits");
    _max_file_size = readInt("basic.maxFilesize");
    _max_open_files = readInt("basic.maxOpenfiles");
    _hierarchical_bloom_flag = readBool("basic.hierarchicalBoomflag");
    _open_log = readBool("basic.openLog");
    _compression_flag = readBool("basic.compressionFlag");
}
/*template<typename T>
boost::shared_ptr<T> Basic_ConfigMod<T>::instance= nullptr;*/
std::string LevelDB_ConfigMod::getBloom_filename(){
    assert(!_pt.empty());
    return _bloom_filename;
}

int LevelDB_ConfigMod::getBloom_bits(){
    assert(!_pt.empty());
    return _bloom_bits;
}

int LevelDB_ConfigMod::getMax_file_size(){
    assert(!_pt.empty());
    return _max_file_size;
}

int LevelDB_ConfigMod::getMax_open_files(){
    assert(!_pt.empty());
    return _max_open_files;
}

bool LevelDB_ConfigMod::getHierarchical_bloom_flag(){
    assert(!_pt.empty());
    return _hierarchical_bloom_flag;
}

bool LevelDB_ConfigMod::getOpen_log(){
     assert(!_pt.empty());
     return _open_log;
}

bool LevelDB_ConfigMod::getCompression_flag(){
    assert(!_pt.empty());
    return _compression_flag;
}
template<>
boost::shared_ptr<LevelDB_ConfigMod> Basic_ConfigMod<LevelDB_ConfigMod>::instance = nullptr;	