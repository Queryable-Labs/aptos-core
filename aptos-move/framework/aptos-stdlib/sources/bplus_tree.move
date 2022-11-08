module aptos_std::bplus_tree {
    use std::vector;
    use aptos_std::table_with_length::{Self, TableWithLength};

    const E_UNKNOWN: u64 = 0;

    const E_INVALID_ARGUMENT: u64 = 1;
    const E_KEY_ALREADY_EXIST: u64 = 2;
    const E_EMPTY_TREE: u64 = 3;
    const E_INVALID_INDEX: u64 = 4;
    const E_TREE_TOO_BIG: u64 = 5;
    const E_TREE_NOT_EMPTY: u64 = 6;

    // NULL_INDEX is 1 << 64 - 1 (all 1s for the 64 bits);
    const NULL_INDEX: u64 = 18446744073709551615;

    // check if the index is NULL_INDEX
    public fun is_null_index(index: u64): bool {
        index == NULL_INDEX
    }

    const DEFAULT_ORDER : u8 = 32;

    struct Node has drop, store {
        is_leaf: bool,
        parent: u64,
        key_indices: vector<u64>,
        children_or_value: vector<u64>,
        prev_node: u64,
        next_node: u64,
    }

    fun new_node(is_leaf: bool, parent: u64): Node {
        Node {
            is_leaf: is_leaf,
            parent: parent,
            key_indices: vector::empty(),
            children_or_value: vector::empty(),
            prev_node: NULL_INDEX,
            next_node: NULL_INDEX,
        }
    }

    struct BplusTree<V> has store {
        root: u64,
        nodes: TableWithLength<u64, Node>,
        entries: TableWithLength<u64, V>,
        order: u8,
        min_index: u64,
        max_index: u64,
    }

    /// create new tree
    public fun new<V: store>(): BplusTree<V> {
        new_with_order(DEFAULT_ORDER)
    }

    public fun new_with_order<V: store>(order: u8): BplusTree<V> {
        BplusTree {
            root: NULL_INDEX,
            nodes: table_with_length::new(),
            entries: table_with_length::new(),
            order: order,
            min_index: NULL_INDEX,
            max_index: NULL_INDEX,
        }
    }

    ///////////////
    // Accessors //
    ///////////////

    /// Returns the element index in the BplusTree, or NULL_INDEX if not found.
    public fun find<V>(tree: &BplusTree<V>, key: u64): u64 {
        let leaf = find_leaf(tree, key);
        if (leaf == NULL_INDEX) {
            return NULL_INDEX
        };

        let node = table_with_length::borrow(&tree.nodes, leaf);
        assert!(node.is_leaf, E_UNKNOWN);

        let len = vector::length(&node.key_indices);
        let i = 0;
        while (i < len) {
            if (*vector::borrow(&node.key_indices, i) == key) {
                return *vector::borrow(&node.children_or_value, i)
            }
        };

        NULL_INDEX
    }

    fun find_leaf<V>(tree: &BplusTree<V>, key: u64): u64 {
        let current = tree.root;

        while (current != NULL_INDEX) {
            let node = table_with_length::borrow(&tree.nodes, current);
            if (node.is_leaf) {
                return current
            };
            let len = vector::length(&node.key_indices);
            if (len == 0 || *vector::borrow(&node.key_indices, len - 1) < key) {
                return NULL_INDEX
            };
            let i = 0;
            while (i < len) {
                if (*vector::borrow(&node.key_indices, i) >= key) {
                    current = *vector::borrow(&node.children_or_value, i);
                    break
                }
            }
        };

        NULL_INDEX
    }

    /// Returns a reference to the element with its key at the given index.
    public fun borrow_at_index<V>(tree: &BplusTree<V>, index: u64): &V {
        table_with_length::borrow(&tree.entries, index)
    }

    /// Returns a mutable reference to the element with its key at the given index
    public fun borrow_at_index_mut<V>(tree: &mut BplusTree<V>, index: u64): &mut V {
        table_with_length::borrow_mut(&mut tree.entries, index)
    }

    /// Returns the number of elements in the BplusTree.
    public fun size<V>(tree: &BplusTree<V>): u64 {
        table_with_length::length(&tree.entries)
    }

    /// Returns true iff the BplusTree is empty.
    public fun empty<V>(tree: &BplusTree<V>): bool {
        table_with_length::length(&tree.entries) == 0
    }

    /// get index of the min of the tree.
    public fun get_min_index<V>(tree: &BplusTree<V>): u64 {
        let current = tree.min_index;
        assert!(current != NULL_INDEX, E_EMPTY_TREE);
        current
    }

    /// get index of the min of the subtree with root at index.
    public fun get_min_index_from<V>(tree: &BplusTree<V>, index: u64): u64 {
        let current = index;

        current
    }

    /// get index of the max of the tree.
    public fun get_max_index<V>(tree: &BplusTree<V>): u64 {
        let current = tree.max_index;
        assert!(current != NULL_INDEX, E_EMPTY_TREE);
        current
    }

    /// get index of the max of the subtree with root at index.
    public fun get_max_index_from<V>(tree: &BplusTree<V>, index: u64): u64 {
        let current = index;

        current
    }

    /// find next value in order (the key is increasing)
    public fun next_in_order<V>(tree: &BplusTree<V>, index: u64): u64 {
        assert!(index != NULL_INDEX, E_INVALID_INDEX);
        NULL_INDEX
    }

    /// find next value in reverse order (the key is decreasing)
    public fun next_in_reverse_order<V>(tree: &BplusTree<V>, index: u64): u64 {
        assert!(index != NULL_INDEX, E_INVALID_INDEX);
        NULL_INDEX
    }

    ///////////////
    // Modifiers //
    ///////////////

    /// Puts the value keyed at the input keys into the BplusTree.
    /// Aborts if the key is already in the tree.
    public fun insert<V>(tree: &mut BplusTree<V>, key: u64, value: V) {
        assert!(size(tree) < 1000000, E_TREE_TOO_BIG);

        table_with_length::add(&mut tree.entries, key, value);

        let leaf = find_leaf(tree, key);

        if (leaf == NULL_INDEX) {
            leaf = tree.max_index;
        };

        insert_at(tree, leaf, key);
    }

    fun insert_at<V>(tree: &mut BplusTree<V>, node_index: u64, key: u64) {
        let node = table_with_length::borrow_mut(&mut tree.nodes, node_index);
        let keys = &mut node.key_indices;
        let current_size = vector::length(keys);

        if (current_size < (tree.order as u64)) {
            let i = current_size;
            vector::push_back(keys, 0);
            while (i > 0) {
                let previous_key = *vector::borrow(keys, i - 1);
                if (previous_key < key) {
                    break
                };
                *vector::borrow_mut(keys, i) = previous_key;
                i = i - 1;
            };
            *vector::borrow_mut(keys, i) = key;
            return
        };

        let target_size = ((tree.order as u64) + 1) / 2;

        let l = 0;
        let r = current_size;
        while (l != r) {
            let mid = l + (r - l) / 2;
            if (key < *vector::borrow(keys, mid)) {
                l = mid + 1;
            } else {
                r = mid;
            };
        };

        let parent = node.parent;
        let brother_node = new_node(node.is_leaf, parent);
        if (l < target_size) {
            let i = target_size - 1;
            while (i < current_size) {
                vector::push_back(&mut brother_node.key_indices, *vector::borrow(keys, i));
                i = i + 1;
                vector::pop_back(keys);
            };
            vector::push_back(keys, 0);
            i = target_size - 1;
            while (i > l) {
                *vector::borrow_mut(keys, i) = *vector::borrow(keys, i - 1);
            };
            *vector::borrow_mut(keys, l) = key;
        } else {
            let i = target_size;
            while (i < l) {
                vector::push_back(&mut brother_node.key_indices, *vector::borrow(keys, i));
                i = i + 1;
                vector::pop_back(keys);
            };
            vector::push_back(&mut brother_node.key_indices, key);
            while (i < current_size) {
                vector::push_back(&mut brother_node.key_indices, *vector::borrow(keys, i));
                i = i + 1;
                vector::pop_back(keys);
            }
        };

        move node;

        let node_index = table_with_length::length(&tree.nodes) + 1;
        table_with_length::add(&mut tree.nodes, node_index, brother_node);
        insert_at(tree, parent, 0);
    }

    /// Removes the entry from BplusTree and returns the value which `key` maps to.
    /// Aborts if there is no entry for `key`.
    public fun remove<V>(tree: &mut BplusTree<V>, key: u64): V {
        let value = table_with_length::remove(&mut tree.entries, key);
        let leaf = find_leaf(tree, key);
        assert!(leaf != NULL_INDEX, E_UNKNOWN);

        remove_at(tree, leaf, key);
        value
    }

    fun remove_at<V>(tree: &mut BplusTree<V>, node_index: u64, key: u64) {
        let node = table_with_length::remove(&mut tree.nodes, node_index);
        let prev_node = node.prev_node;
        let next_node = node.next_node;

        let keys = &mut node.key_indices;

        let current_size = vector::length(keys);

        let l = 0;
        let r = current_size;

        while (l != r) {
            let mid = l + (r - l) / 2;
            if (key < *vector::borrow(keys, mid)) {
                l = mid + 1;
            } else {
                r = mid;
            };
        };

        assert!(*vector::borrow(keys, l) == key, E_UNKNOWN);
        while (l < current_size - 1) {
            *vector::borrow_mut(keys, l) = *vector::borrow(keys, l + 1);
            l = l + 1;
        };
        vector::pop_back(keys);

        current_size = current_size - 1;
        if (current_size * 2 < (tree.order as u64) && leaf != tree.root) {
            let brother = next_node;
            if (brother == NULL_INDEX) {
                brother = prev_node;
            };
            let brother_node = table_with_length::remove(&mut tree.nodes, brother);
            let brother_keys = &mut brother_node.key_indices;
            let brother_size = vector::length(brother_keys);
            if ((brother_size - 1) * 2 >= (tree.order as u64)) {
                if (brother == next_node) {
                    vector::push_back(keys, *vector::borrow(brother_keys, 0));
                    brother_size = brother_size - 1;
                    let i = 0;
                    while (i < brother_size) {
                        *vector::borrow_mut(brother_keys, i) = *vector::borrow(brother_keys, i + 1);
                    };
                    vector::pop_back(brother_keys);
                    // TODO: Update parent.
                } else {
                    vector::push_back(keys, 0);
                    let i = current_size;
                    while (i > 0) {
                        *vector::borrow_mut(keys, i) = *vector::borrow(keys, i - 1);
                    };
                    *vector::borrow_mut(keys, 0) = vector::pop_back(brother_keys); 
                    // TODO: Update parent.
                }
            } else {
                if (brother == next_node) {
                    vector::append(keys, brother_node.key_indices);
                } else {
                    vector::append(brother_keys, node.key_indices);
                }
            }
        };

        value
    }

    /// destroys the tree if it's empty.
    public fun destroy_empty<V>(tree: BplusTree<V>) {
        let BplusTree { entries, nodes, order: _, root: _, min_index: _, max_index: _ } = tree;
        assert!(table_with_length::empty(&entries), E_TREE_NOT_EMPTY);
        assert!(table_with_length::empty(&nodes), E_TREE_NOT_EMPTY);
        table_with_length::destroy_empty(entries);
        table_with_length::destroy_empty(nodes);
    }
}
