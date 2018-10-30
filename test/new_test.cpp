// This test program performs a series of inserts, deletes, updates,
// and queries to a betree.  It performs the same sequence of
// operatons on a std::map.  It checks that it always gets the same
// result from both data structures.

// The program takes 1 command-line parameter -- the number of
// distinct keys it can use in the test.

// The values in this test are strings.  Since updates use operator+

// on the values, this test performs concatenation on the strings.

#include <string.h>
#include <sys/types.h>
#include <sys/time.h>
#include <unistd.h>
#include <serializable_types.hpp>
#include <betree.hpp>

#define DEFAULT_TEST_MAX_NODE_SIZE (1ULL<<6)
#define DEFAULT_TEST_MIN_FLUSH_SIZE (DEFAULT_TEST_MAX_NODE_SIZE / 4)
#define DEFAULT_TEST_CACHE_SIZE (4)
#define DEFAULT_TEST_NDISTINCT_KEYS (1ULL << 10)
#define DEFAULT_TEST_NOPS (1ULL << 12)

SerializableString interleave(SerializableString &v, std::vector<std::shared_ptr<serializable>> arg) {	
	std::shared_ptr<SerializableString> s = std::dynamic_pointer_cast<SerializableString>(arg[0]);
	
	std::string new_v(s->size()+v.size(), '*');

	unsigned int vp{0};
	unsigned int sp{0};

	int i{0};	
	while(sp < s->size() || vp < v.size()) {
		if(vp < v.size()) { 
			new_v[i] = v.at(vp);
			++i;
		}
		
		if(sp < s->size()) {
			new_v[i] = s->at(sp);
			++i;
		}
		
		++vp;
		++sp; 
	}
	return SerializableString(new_v);
}

std::string ref_interleave(std:: string &v, std::string &s) {
	std::string new_v = "";
	unsigned int vp{0};
  unsigned int sp{0};

  while(sp < s.size() || vp < v.size()) {
    if(vp < v.size())
      new_v += v[vp];
    if(sp < s.size())
      new_v += s[sp];

    ++vp;
    ++sp;
  }

  return new_v;
}

int main(int argc, char **argv)
{
		uint64_t low = 100;
		uint64_t high = 10110;	
	
    uint64_t max_node_size = DEFAULT_TEST_MAX_NODE_SIZE;
    uint64_t min_flush_size = DEFAULT_TEST_MIN_FLUSH_SIZE;
    uint64_t cache_size = DEFAULT_TEST_CACHE_SIZE;
    char *backing_store_dir = NULL;
    unsigned int random_seed = time(NULL) * getpid();
    
    int opt;
    
    while ((opt = getopt(argc, argv, "d:")) != -1) {
        switch (opt) {
            case 'd':
                backing_store_dir = optarg;
                break;
            default:
                std::cerr << "Unknown option '" << (char)opt << "'" << std::endl;
                exit(1);
        }
    }
    
    srand(random_seed);
    
    if (backing_store_dir == NULL) {
        std::cerr << "-d <backing_store_directory> is required" << std::endl;
        exit(1);
    }
    
    ////////////////////////////////////////////////////////
    // Construct a betree and run the tests or benchmarks //
    ////////////////////////////////////////////////////////
	    
    one_file_per_object_backing_store ofpobs(backing_store_dir);
    swap_space sspace(&ofpobs, cache_size);
    betree<uint64_t, SerializableString> b(&sspace, max_node_size, min_flush_size);
		std::map<uint64_t, std::string> reference;

		std::string *ref_u = new std::string("interleave");
		std::shared_ptr<SerializableString>bet_u = std::make_shared<SerializableString>(*ref_u);
		
		b.register_function("test", &interleave);		
	
		for(uint64_t i = low; i < high; ++i) {
			std::vector<std::shared_ptr<serializable>> params;
			params.push_back(std::static_pointer_cast<serializable>(
					std::make_shared<SerializableString>(std::to_string(i))));
			reference.insert(make_pair(i, std::to_string(i)));
			b.insert(i,params);
		}

		for(uint64_t i = low; i < high; ++i) {
			std::cout << "reference: " << i << " -> " << reference[i] << std::endl;
			std::cout << "betree   : " << i << " -> " << b.query(i) << std::endl; 
		}

		for(uint64_t i = low; i < high; ++i) {
			reference[i] = ref_interleave(reference[i], *ref_u);
			std::vector<std::shared_ptr<serializable>>params;
			params.push_back(std::static_pointer_cast<serializable>(bet_u));
			b.upsert("test", i, params);
		}
		
		std::cout << "--------------------" << std::endl;

		for(uint64_t i = low; i < high; ++i) {
			std::cout << "reference: " << i << " -> " << reference[i] << std::endl;
			std::cout << "betree   : " << i << " -> " << b.query(i) << std::endl; 
		}		

		b.deregister_function("update");
	 
    return 0;
}

