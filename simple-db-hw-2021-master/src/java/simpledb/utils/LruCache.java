package simpledb.utils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class LruCache<K, V> {
    // LruCache Node
    public class Node<K, V> {
        public Node pre;
        public Node next;
        public K key;
        public V value;

        public Node(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

    private final int maxSize;
    private final Map<K, Node> nodeMap;
    private final Node head;
    private final Node tail;

    public LruCache(int maxSize) {
        this.maxSize = maxSize;
        this.head = new Node(null, null);
        this.tail = new Node(null, null);
        this.head.next = tail;
        this.tail.pre = head;
        this.nodeMap = new ConcurrentHashMap<>();
    }

    /**
     * Add a node to the head of the linked list.
     *
     * @param node : A new node.
     */
    public void addToHead(Node node) {
        Node p = head.next;
        node.next = p;
        node.pre = head;
        head.next = node;
        p.pre = node;
    }

    /**
     * Move an existing node in the linked list to the head of the list.
     *
     * @param node : Node to move.
     */
    public void moveToHead(Node node) {
        removeNode(node);
        addToHead(node);
    }

    /**
     * Delete a node in a linked list.
     *
     * @param node
     */
    public void removeNode(Node node) {
        if (node.next != null && node.pre != null) {
            node.pre.next = node.next;
            node.next.pre = node.pre;
        }
    }

    public synchronized void remove(K key) {
        if (nodeMap.containsKey(key)) {
            Node node = nodeMap.get(key);
            removeNode(node);
            nodeMap.remove(key);
        }
    }

    /**
     * Get the value corresponding to the key,
     * and move the node corresponding to the key to the head of the linked list.
     *
     * @param key
     * @return : The value corresponding to the key.
     */
    public synchronized V get(K key) {
        if (nodeMap.containsKey(key)) {
            Node node = nodeMap.get(key);
            moveToHead(node);
            return (V) node.value;
        }
        return null;
    }

    /**
     * Insert a new node into the cache.
     *
     * @param key
     * @param value
     */
    public synchronized void put(K key, V value) {
        if (nodeMap.containsKey(key)) {
            moveToHead(nodeMap.get(key));
        } else {
            Node node = new Node(key, value);
            nodeMap.put(key, node);
            addToHead(node);
        }
    }

    public synchronized Iterator<V> reverseIterator() {
        Node last = tail.pre;
        List<V> list = new ArrayList<>();
        while (!last.equals(head)) {
            list.add((V) last.value);
            last = last.pre;
        }
        return list.iterator();
    }

    public synchronized Iterator<K> valueIterator() {
        Collection<Node> nodes = nodeMap.values();
        List<K> valueList = new ArrayList<>();
        for (Node node : nodes) {
            valueList.add((K) node.key);
        }
        return valueList.iterator();
    }

    public synchronized int getSize() {
        return nodeMap.size();
    }

    public int getMaxSize() {
        return maxSize;
    }

}
