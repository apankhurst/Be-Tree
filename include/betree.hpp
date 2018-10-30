// FIX MEMORY LEAKS WITH SHARED PTRS????

// A basic B^e-tree implementation templated on types Key and Value.
// Keys and Values must be serializable (see swap_space.hpp).
// Keys must be comparable (via operator< and operator==).
// Values must be addable (via operator+).
// See test.cpp for example usage.

// This implementation represents in-memory nodes as objects with two
// fields:
// - a std::map mapping keys to child pointers
// - a std::map mapping (key, timestamp) pairs to messages
// Nodes are de/serialized to/from an on-disk representation.
// I/O is managed transparently by a swap_space object.

// This implementation deviates from a "textbook" implementation in
// that there is not a fixed division of a node's space between pivots
// and buffered messages.

// In a textbook implementation, nodes have size B, B^e space is
// devoted to pivots and child pointers, and B-B^e space is devoted to
// buffering messages.  Whenever a leaf gets too many messages, it
// splits.  Whenever an internal node gets too many messages, it
// performs a flush.  Whenever an internal node gets too many
// children, it splits.  This policy ensures that, whenever the tree
// needs to flush messages from a node to one of its children, it can
// always move a batch of size at least (B-B^e) / B^e = B^(1-e) - 1
// messages.

// In this implementation, nodes have a fixed maximum size.  Whenever
// a leaf exceeds this max size, it splits.  Whenever an internal node
// exceeds this maximum size, it checks to see if it can flush a large
// batch of elements to one of its children.  If it can, it does so.
// If it cannot, then it splits.

// In-memory nodes may temporarily exceed the maximum size
// restriction.  During a flush, we move all the incoming messages
// into the destination node.  At that point the node may exceed the
// max size.  The flushing procedure then performs further flushes or
// splits to restore the max-size invariant.  Thus, whenever a flush
// returns, all the nodes in the subtree of that node are guaranteed
// to satisfy the max-size requirement.

// This implementation also optimizes I/O based on which nodes are
// on-disk, clean in memory, or dirty in memory.  For example,
// inserted items are always immediately flushed as far down the tree
// as they can go without dirtying any new nodes.  This is because
// flushing an item to a node that is already dirty will not require
// any additional I/O, since the node already has to be written back
// anyway.  Furthermore, it will flush smaller batches to clean
// in-memory nodes than to on-disk nodes.  This is because dirtying a
// clean in-memory node only requires a write-back, whereas flushing
// to an on-disk node requires reading it in and writing it out.

#include <map>
#include <vector>
#include <cassert>
#include <string>
#include <memory>
#include "serializable_types.hpp"
#include "swap_space.hpp"
#include "backing_store.hpp"

////////////////// Upserts

// Internally, we store data indexed by both the user-specified key
// and a timestamp, so that we can apply upserts in the correct order.
template<class Key>
class MessageKey {
public:
    MessageKey(void) :
    key(),
    timestamp(0)
    {}
    
    MessageKey(const Key & k, uint64_t tstamp) :
    key(k),
    timestamp(tstamp)
    {}
    
    static MessageKey range_start(const Key &key) {
        return MessageKey(key, 0);
    }
    
    static MessageKey range_end(const Key &key) {
        return MessageKey(key, UINT64_MAX);
    }
    
    MessageKey range_start(void) const {
        return range_start(key);
    }
    
    MessageKey range_end(void) const {
        return range_end(key);
    }
    
    void _serialize(std::iostream &fs, serialization_context &context) const {
        fs << timestamp << " ";
        serialize(fs, context, key);
    }
    
    void _deserialize(std::iostream &fs, serialization_context &context) {
        fs >> timestamp;
        deserialize(fs, context, key);
    }
    
    Key key;
    uint64_t timestamp;
};

template<class Key>
bool operator<(const MessageKey<Key> & mkey1, const MessageKey<Key> & mkey2) {
    return mkey1.key < mkey2.key ||
    (mkey1.key == mkey2.key && mkey1.timestamp < mkey2.timestamp);
}

template<class Key>
bool operator<(const Key & key, const MessageKey<Key> & mkey) {
    return key < mkey.key;
}

template<class Key>
bool operator<(const MessageKey<Key> & mkey, const Key & key) {
    return mkey.key < key;
}

template<class Key>
bool operator==(const MessageKey<Key> &a, const MessageKey<Key> &b) {
    return a.key == b.key && a.timestamp == b.timestamp;
}

// The three types of upsert.  An UPDATE specifies a value, v, that
// will be added (using operator+) to the old value associated to some
// key in the tree.  If there is no old value associated with the key,
// then it will add v to the result of a Value obtained using the
// default zero-argument constructor.
class Message {
public:
	Message(): type(0), func("insert"), params() {}
	
	Message(int t, std::string f, std::vector<std::shared_ptr<serializable>> p):
		type(t),
		func(f),
		params(p)
	{}

	~Message() {
		//for(auto &p : params) 
		//	delete p;
	}

	void _serialize(std::iostream &fs, serialization_context &context) {
		fs << type << " " << func << " ";
		for(auto &p : params) {
			p->_serialize(fs, context);
			fs << " "; 
		}
	} 

	void _deserialize(std::iostream &fs, serialization_context &context) {
		std::string p_type;
		std::string dummy;
		fs >> dummy;
		type = std::stoi(dummy);
		fs >> func;
		std::getline(fs, dummy);
		std::stringstream ss(dummy);
		while(ss >> p_type) {
			if(p_type == "string") {
				std::shared_ptr<serializable> s = std::make_shared<SerializableString>();
				s->_deserialize(ss, context);
				params.push_back(s);
			}	
		}
	}

	int type;
	std::string func;
	std::vector<std::shared_ptr<serializable>> params;
};


// Messages are only the same iff the *address* of their parameters are
// the same
bool operator==(const Message &a, const Message &b) {
    return a.func == b.func && a.params == b.params;
}

// Measured in messages.
#define DEFAULT_MAX_NODE_SIZE (1ULL<<18)

// The minimum number of messages that we will flush to an out-of-cache node.
// Note: we will flush even a single element to a child that is already dirty.
// Note: we will flush MIN_FLUSH_SIZE/2 items to a clean in-memory child.
#define DEFAULT_MIN_FLUSH_SIZE (DEFAULT_MAX_NODE_SIZE / 16ULL)


template<class Key, class Value> class betree {
private:
    
    class node;
    // We let a swap_space handle all the I/O.
    typedef typename swap_space::pointer<node> node_pointer;
    class child_info : public serializable {
    public:
        child_info(void)
        : child(),
        child_size(0)
        {}
        
        child_info(node_pointer child, uint64_t child_size)
        : child(child),
        child_size(child_size)
        {}
        
        void _serialize(std::iostream &fs, serialization_context &context) {
            serialize(fs, context, child);
            fs << " ";
            serialize(fs, context, child_size);
        }
        
        void _deserialize(std::iostream &fs, serialization_context &context) {
            deserialize(fs, context, child);
            deserialize(fs, context, child_size);
        }
        
        node_pointer child;
        uint64_t child_size;
    };
    
		typedef typename std::map<Key, child_info> pivot_map;
    // Message Used Here
		typedef typename std::map<MessageKey<Key>, Message > message_map;
    
    class node : public serializable {
    public:
        
        // Child pointers
        pivot_map pivots;
        message_map elements;
        
        bool is_leaf(void) const {
            return pivots.empty();
        }
        
        // Holy frick-a-moly.  We want to write a const function that
        // returns a const_iterator when called from a const function and
        // a non-const function that returns a (non-const_)iterator when
        // called from a non-const function.  And we don't want to
        // duplicate the code.  The following solution is from
        //         http://stackoverflow.com/a/858893
        template<class OUT, class IN>
        static OUT get_pivot(IN & mp, const Key & k) {
            assert(mp.size() > 0);
            auto it = mp.lower_bound(k);
            if (it == mp.begin() && k < it->first)
                throw std::out_of_range("Key does not exist "
                                        "(it is smaller than any key in DB)");
            if (it == mp.end() || k < it->first)
                --it;
            return it;
        }
        
        // Instantiate the above template for const and non-const
        // calls. (template inference doesn't seem to work on this code)
        typename pivot_map::const_iterator get_pivot(const Key & k) const {
            return get_pivot<typename pivot_map::const_iterator,
            const pivot_map>(pivots, k);
        }
        
        typename pivot_map::iterator
        get_pivot(const Key & k) {
            return get_pivot<typename pivot_map::iterator, pivot_map>(pivots, k);
        }
        
        // Return iterator pointing to the first element with mk >= k.
        // (Same const/non-const templating trick as above)
        template<class OUT, class IN>
        static OUT get_element_begin(IN & elts, const Key &k) {
            return elts.lower_bound(MessageKey<Key>::range_start(k));
        }
        
        typename message_map::iterator get_element_begin(const Key &k) {
            return get_element_begin<typename message_map::iterator,
            message_map>(elements, k);
        }
        
        typename message_map::const_iterator get_element_begin(const Key &k) const {
            return get_element_begin<typename message_map::const_iterator,
            const message_map>(elements, k);
        }
        
        // Return iterator pointing to the first element that goes to
        // child indicated by it
        typename message_map::iterator
        get_element_begin(const typename pivot_map::iterator it) {
            return it == pivots.end() ? elements.end() : get_element_begin(it->first);
        }
        
       // Message Used Here 
        // Apply a message to ourself.
        void apply(betree &bet, const MessageKey<Key> &mkey, const Message &elt,
                   Value &default_value) {
            if(elt.func == "insert") {
                elements.erase(elements.lower_bound(mkey.range_start()),
                               elements.upper_bound(mkey.range_end()));
                elements[mkey] = elt;
            } else if(elt.func == "delete") {
                elements.erase(elements.lower_bound(mkey.range_start()),
                               elements.upper_bound(mkey.range_end()));
                if (!is_leaf())
                    elements[mkey] = elt;
            } else {
                if(!bet.has_function(elt.func))
                    assert(0);
                auto iter = elements.upper_bound(mkey.range_end());
                if (iter != elements.begin())
                    iter--;
                if (iter == elements.end() || iter->first.key != mkey.key) {
                    if (is_leaf()) {
												// as is this leaks
                        Value dummy = default_value;
												std::shared_ptr<Value> v = std::make_shared<Value>();
												*v = bet.function_map[elt.func](dummy, elt.params);
												std::vector<std::shared_ptr<serializable>> p;
												p.push_back(std::static_pointer_cast<serializable>(v));
                        apply(bet, mkey, Message(0, "insert", p),
                              default_value);
                    } else {
                        elements[mkey] = elt;
                    }
                } else {
                	assert(iter != elements.end() && iter->first.key == mkey.key);
                  std::shared_ptr<Value> v = std::make_shared<Value>();
									*v = bet.call_function(elt.func, *(std::dynamic_pointer_cast<Value>(iter->second.params[0])), elt.params);
									std::vector<std::shared_ptr<serializable>> p;
									p.push_back(std::static_pointer_cast<serializable>(v));
									if (iter->second.func == "insert") {
                  	apply(bet, mkey, Message(0, "insert", p),
                                  default_value);
                  } else {
                    elements[mkey] = elt;
                  }
               }
            }
        }
        
        // Requires: there are less than MIN_FLUSH_SIZE things in elements
        //           destined for each child in pivots);
        pivot_map split(betree &bet) {
            assert(pivots.size() + elements.size() >= bet.max_node_size);
            // This size split does a good job of causing the resulting
            // nodes to have size between 0.4 * MAX_NODE_SIZE and 0.6 * MAX_NODE_SIZE.
            int num_new_leaves =
            (pivots.size() + elements.size())  / (10 * bet.max_node_size / 24);
            int things_per_new_leaf =
            (pivots.size() + elements.size() + num_new_leaves - 1) / num_new_leaves;
            
            pivot_map result;
            auto pivot_idx = pivots.begin();
            auto elt_idx = elements.begin();
            int things_moved = 0;
            for (int i = 0; i < num_new_leaves; i++) {
                if (pivot_idx == pivots.end() && elt_idx == elements.end())
                    break;
                node_pointer new_node = bet.ss->allocate(new node());
                result[pivot_idx != pivots.end() ?
                       pivot_idx->first :
                       elt_idx->first.key] = child_info(new_node,
                                                        new_node->elements.size() +
                                                        new_node->pivots.size());
                while(things_moved < (i+1) * things_per_new_leaf &&
                      (pivot_idx != pivots.end() || elt_idx != elements.end())) {
                    if (pivot_idx != pivots.end()) {
                        new_node->pivots[pivot_idx->first] = pivot_idx->second;
                        ++pivot_idx;
                        things_moved++;
                        auto elt_end = get_element_begin(pivot_idx);
                        while (elt_idx != elt_end) {
                            new_node->elements[elt_idx->first] = elt_idx->second;
                            ++elt_idx;
                            things_moved++;
                        }
                    } else {
                        // Must be a leaf
                        assert(pivots.size() == 0);
                        new_node->elements[elt_idx->first] = elt_idx->second;
                        ++elt_idx;
                        things_moved++;
                    }
                }
            }
            
            for (auto it = result.begin(); it != result.end(); ++it)
                it->second.child_size = it->second.child->elements.size() +
                it->second.child->pivots.size();
            
            assert(pivot_idx == pivots.end());
            assert(elt_idx == elements.end());
            pivots.clear();
            elements.clear();
            return result;
        }
        
        node_pointer merge(betree &bet,
                           typename pivot_map::iterator begin,
                           typename pivot_map::iterator end) {
            node_pointer new_node = bet.ss->allocate(new node(bet));
            for (auto it = begin; it != end; ++it) {
                new_node->elements.insert(it->second.child->elements.begin(),
                                          it->second.child->elements.end());
                new_node->pivots.insert(it->second.child->pivots.begin(),
                                        it->second.child->pivots.end());
            }
            return new_node;
        }
        
        void merge_small_children(betree &bet) {
            if (is_leaf())
                return;
            
            for (auto beginit = pivots.begin(); beginit != pivots.end(); ++beginit) {
                uint64_t total_size = 0;
                auto endit = beginit;
                while (endit != pivots.end()) {
                    if (total_size + beginit->second.child_size > 6 * bet.max_node_size / 10)
                        break;
                    total_size += beginit->second.child_size;
                    ++endit;
                }
                if (endit != beginit) {
                    node_pointer merged_node = merge(bet, beginit, endit);
                    for (auto tmp = beginit; tmp != endit; ++tmp) {
                        tmp->second.child->elements.clear();
                        tmp->second.child->pivots.clear();
                    }
                    Key key = beginit->first;
                    pivots.erase(beginit, endit);
                    pivots[key] = child_info(merged_node, merged_node->pivots.size() + merged_node->elements.size());
                    beginit = pivots.lower_bound(key);
                }
            }
        }
        
        // Receive a collection of new messages and perform recursive
        // flushes or splits as necessary.  If we split, return a
        // map with the new pivot keys pointing to the new nodes.
        // Otherwise return an empty map.
        pivot_map flush(betree &bet, message_map &elts)
        {
            debug(std::cout << "Flushing " << this << std::endl);
            pivot_map result;
            
            if (elts.size() == 0) {
                debug(std::cout << "Done (empty input)" << std::endl);
                return result;
            }
            
            if (is_leaf()) {
                for (auto it = elts.begin(); it != elts.end(); ++it)
                    apply(bet, it->first, it->second, bet.default_value);
                if (elements.size() + pivots.size() >= bet.max_node_size)
                    result = split(bet);
                return result;
            }
            
            ////////////// Non-leaf
            
            // Update the key of the first child, if necessary
            Key oldmin = pivots.begin()->first;
            MessageKey<Key> newmin = elts.begin()->first;
            if (newmin < oldmin) {
                pivots[newmin.key] = pivots[oldmin];
                pivots.erase(oldmin);
            }
            
            // If everything is going to a single dirty child, go ahead
            // and put it there.
            auto first_pivot_idx = get_pivot(elts.begin()->first.key);
            auto last_pivot_idx = get_pivot((--elts.end())->first.key);
            if (first_pivot_idx == last_pivot_idx &&
                first_pivot_idx->second.child.is_dirty()) {
                // There shouldn't be anything in our buffer for this child,
                // but lets assert that just to be safe.
                {
                    auto next_pivot_idx = next(first_pivot_idx);
                    auto elt_start = get_element_begin(first_pivot_idx);
                    auto elt_end = get_element_begin(next_pivot_idx);
                    assert(elt_start == elt_end);
                }
                pivot_map new_children = first_pivot_idx->second.child->flush(bet, elts);
                if (!new_children.empty()) {
                    pivots.erase(first_pivot_idx);
                    pivots.insert(new_children.begin(), new_children.end());
                } else {
                    first_pivot_idx->second.child_size =
                    first_pivot_idx->second.child->pivots.size() +
                    first_pivot_idx->second.child->elements.size();
                }
                
            } else {
                
                for (auto it = elts.begin(); it != elts.end(); ++it)
                    apply(bet, it->first, it->second, bet.default_value);
                
                // Now flush to out-of-core or clean children as necessary
                while (elements.size() + pivots.size() >= bet.max_node_size) {
                    // Find the child with the largest set of messages in our buffer
                    unsigned int max_size = 0;
                    auto child_pivot = pivots.begin();
                    auto next_pivot = pivots.begin();
                    for (auto it = pivots.begin(); it != pivots.end(); ++it) {
                        auto it2 = next(it);
                        auto elt_it = get_element_begin(it);
                        auto elt_it2 = get_element_begin(it2);
                        unsigned int dist = distance(elt_it, elt_it2);
                        if (dist > max_size) {
                            child_pivot = it;
                            next_pivot = it2;
                            max_size = dist;
                        }
                    }
                    if (!(max_size > bet.min_flush_size ||
                          (max_size > bet.min_flush_size/2 &&
                           child_pivot->second.child.is_in_memory())))
                        break; // We need to split because we have too many pivots
                    auto elt_child_it = get_element_begin(child_pivot);
                    auto elt_next_it = get_element_begin(next_pivot);
                    message_map child_elts(elt_child_it, elt_next_it);
                    pivot_map new_children = child_pivot->second.child->flush(bet, child_elts);
                    elements.erase(elt_child_it, elt_next_it);
                    if (!new_children.empty()) {
                        pivots.erase(child_pivot);
                        pivots.insert(new_children.begin(), new_children.end());
                    } else {
                        first_pivot_idx->second.child_size =
                        child_pivot->second.child->pivots.size() +
                        child_pivot->second.child->elements.size();
                    }
                }
                
                // We have too many pivots to efficiently flush stuff down, so split
                if (elements.size() + pivots.size() > bet.max_node_size) {
                    result = split(bet);
                }
            }
            
            //merge_small_children(bet);
            
            debug(std::cout << "Done flushing " << this << std::endl);
            return result;
        }
        
        Value query(betree & bet, const Key k) const
        {
            debug(std::cout << "Querying " << this << std::endl);
            if (is_leaf()) {
                auto it = elements.lower_bound(MessageKey<Key>::range_start(k));
                if (it != elements.end() && it->first.key == k) {
                    assert(it->second.func == "insert");
										return *((std::dynamic_pointer_cast<Value>)(it->second.params[0]));
                } else {
                    throw std::out_of_range("Key does not exist");
                }
            }
            
            ///////////// Non-leaf
            auto message_iter = get_element_begin(k);
            Value v = bet.default_value;
            
            if (message_iter == elements.end() || k < message_iter->first) {
                // If we don't have any messages for this key, just search
                // further down the tree.
                v = get_pivot(k)->second.child->query(bet, k);
            } else if (message_iter->second.func == "delete") {
                // We have a delete message, so we don't need to look further
                // down the tree.  If we don't have any further update or
                // insert messages, then we should return does-not-exist (in
                // this subtree).
                message_iter++;
                if (message_iter == elements.end() || k < message_iter->first)
                    throw std::out_of_range("Key does not exist");
            } else if (message_iter->second.func == "insert") {
                // We have an insert message, so we don't need to look further
                // down the tree.  We'll apply any updates to this value.
                v = *(std::dynamic_pointer_cast<Value>(message_iter->second.params[0]));
                message_iter++;
            } else {
                // We have some updates for this key.  Search down the tree.
                // If it has something, then apply our updates to that.  If it
                // doesn't have anything, then apply our updates to the
                // default initial value.
                try {
                    Value t = get_pivot(k)->second.child->query(bet, k);
                    v = t;
                } catch (std::out_of_range e) {}
                
                
                // Apply any updates to the value obtained above.
                while (message_iter != elements.end() && message_iter->first.key == k) {
                    assert(message_iter->second.func != "insert" &&
                           message_iter->second.func != "delete");
                    v = bet.call_function(message_iter->second.func,v,message_iter->second.params);
                    message_iter++;
                }
            }
            return v;
        }
        
        std::pair<MessageKey<Key>, Message>
        get_next_message_from_children(const MessageKey<Key> *mkey) const {
            if (mkey && *mkey < pivots.begin()->first)
                mkey = NULL;
            auto it = mkey ? get_pivot(mkey->key) : pivots.begin();
            while (it != pivots.end()) {
                try {
                    return it->second.child->get_next_message(mkey);
                } catch (std::out_of_range e) {}
                ++it;
            }
            throw std::out_of_range("No more messages in any children");
        }
        
        std::pair<MessageKey<Key>, Message >
        get_next_message(const MessageKey<Key> *mkey) const {
            auto it = mkey ? elements.upper_bound(*mkey) : elements.begin();
            
            if (is_leaf()) {
                if (it == elements.end())
                    throw std::out_of_range("No more messages in sub-tree");
                return std::make_pair(it->first, it->second);
            }
            
            if (it == elements.end())
                return get_next_message_from_children(mkey);
            
            try {
                auto kids = get_next_message_from_children(mkey);
                if (kids.first < it->first)
                    return kids;
                else
                    return std::make_pair(it->first, it->second);
            } catch (std::out_of_range e) {
                return std::make_pair(it->first, it->second);
            }
        }
        
        void _serialize(std::iostream &fs, serialization_context &context) {
            fs << "pivots:" << std::endl;
            serialize(fs, context, pivots);
            fs << "elements:" << std::endl;
            serialize(fs, context, elements);
        }
        
        void _deserialize(std::iostream &fs, serialization_context &context) {
            std::string dummy;
            fs >> dummy;
            deserialize(fs, context, pivots);
            fs >> dummy;
            deserialize(fs, context, elements);
        } 
    };
    
    static Value __insert(Value &v, std::vector<std::shared_ptr<serializable>>p) {
        return *(std::dynamic_pointer_cast<Value>(p[0]));
    }
    
    swap_space *ss;
    uint64_t min_flush_size;
    uint64_t max_node_size;
    uint64_t min_node_size;
    node_pointer root;
    uint64_t next_timestamp = 1; // Nothing has a timestamp of 0
    Value default_value;
    std::unordered_map<std::string, Value (*)(Value &, std::vector<std::shared_ptr<serializable>>)> function_map ;
    
public:
    betree(swap_space *sspace,
           uint64_t maxnodesize = DEFAULT_MAX_NODE_SIZE,
           uint64_t minnodesize = DEFAULT_MAX_NODE_SIZE / 4,
           uint64_t minflushsize = DEFAULT_MIN_FLUSH_SIZE) :
    ss(sspace),
    min_flush_size(minflushsize),
    max_node_size(maxnodesize),
    min_node_size(minnodesize),
    function_map()
    {
        root = ss->allocate(new node());
        function_map["insert"] = &__insert;
        function_map["delete"] = nullptr;
    }
    
    void register_function(std::string name, Value (*f)(Value &, std::vector<std::shared_ptr<serializable>>)) {
        if(name != "insert" && name != "delete")
            function_map[name] = f;
    }
    
    void deregister_function(std::string name) {
        if(name != "insert" && name != "delete")
            function_map.erase(name);
    }

		bool has_function(std::string name) {
			return function_map.find(name) != function_map.end();
		}
    

		Value call_function(std::string name, Value &v, std::vector<std::shared_ptr<serializable>> p) {
			if(!has_function(name))
				return Value();
			return function_map[name](v,p);
		}
    // Message Used Here
		// Insert the specified message and handle a split of the root if it
    // occurs.
    void upsert(std::string func, Key k, std::vector<std::shared_ptr<serializable>>params)
    {
        if(function_map.find(func) == function_map.end())
            return;
				
        message_map tmp;
        tmp[MessageKey<Key>(k, next_timestamp++)] = Message(0,func, params);
				pivot_map new_nodes = root->flush(*this, tmp);
        if (new_nodes.size() > 0) {
            root = ss->allocate(new node());
            root->pivots = new_nodes;
        }
    }
    
    void insert(Key k, std::vector<std::shared_ptr<serializable>>params){
			upsert("insert", k, params);
		}
    
    void update(Key k, std::vector<std::shared_ptr<serializable>>params)
    {
        upsert("update", k, params);
    }
    
    void erase(Key k)
    {
        upsert("delete", k, nullptr);
    }
    
    Value query(Key k)
    {
        Value v = root->query(*this, k);
        return v;
    }
   
		// Message Used Here 
    void dump_messages(void) {
        std::pair<MessageKey<Key>, Message> current;
        
        std::cout << "############### BEGIN DUMP ##############" << std::endl;
        
        try {
            current = root->get_next_message(NULL);
            do {
                std::cout << current.first.key       << " "
                << current.first.timestamp << " "
                << current.second.func   << "  0x"
                << current.second.params      << std::endl;
                current = root->get_next_message(&current.first);
            } while (1);
        } catch (std::out_of_range e) {}
    }
    
    class iterator {
    public:
        
        iterator(const betree &bet)
        : bet(bet),
        position(),
        is_valid(false),
        pos_is_valid(false),
        first(),
        second()
        {}
        
        iterator(const betree &bet, const MessageKey<Key> *mkey)
        : bet(bet),
        position(),
        is_valid(false),
        pos_is_valid(false),
        first(),
        second()
        {
            try {
                position = bet.root->get_next_message(mkey);
                pos_is_valid = true;
                setup_next_element();
            } catch (std::out_of_range e) {}
        }
       
				// Message Used Here 
        void apply(const MessageKey<Key> &msgkey, const Message &msg) {
            if(!bet.has_function(msg.func))
                return;
            if(msg.func == "insert") {
                first = msgkey.key;
                second = *((Value *)msg.params);
                is_valid = true;
            } else if (msg.func == "delete") {
                is_valid = true;
            } else {
                first = msgkey.key;
                if (is_valid == false)
                    second = bet.default_value;
                second = bet.function_map[msg.func](second, msg.params);
                is_valid = true;
            }
        }
        
        void setup_next_element(void) {
            is_valid = false;
            while (pos_is_valid && (!is_valid || position.first.key == first)) {
                apply(position.first, position.second);
                try {
                    position = bet.root->get_next_message(&position.first);
                } catch (std::exception e) {
                    pos_is_valid = false;
                }
            }
        }
        
        bool operator==(const iterator &other) {
            return &bet == &other.bet &&
            is_valid == other.is_valid &&
            pos_is_valid == other.pos_is_valid &&
            (!pos_is_valid || position == other.position) &&
            (!is_valid || (first == other.first && second == other.second));
        }
        
        bool operator!=(const iterator &other) {
            return !operator==(other);
        }
        
        iterator &operator++(void) {
            setup_next_element();
            return *this;
        }
        
        const betree &bet;
				// Message Used Here
        std::pair<MessageKey<Key>, Message> position;
        bool is_valid;
        bool pos_is_valid;
        Key first;
        Value second;
    };
    
    iterator begin(void) const {
        return iterator(*this, NULL);
    }
    
    iterator lower_bound(Key key) const {
        MessageKey<Key> tmp = MessageKey<Key>::range_start(key);
        return iterator(*this, &tmp);
    }
    
    iterator upper_bound(Key key) const {
        MessageKey<Key> tmp = MessageKey<Key>::range_end(key);
        return iterator(*this, &tmp);
    }
    
    iterator end(void) const {
        return iterator(*this);
    }
};
