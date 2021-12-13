package org.cossbow.dag;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * DAG有向无环图
 *
 * @param <Node>     顶点唯一key
 * @param <NodeInfo> 顶点名称或描述
 * @param <EdgeInfo> 有向边信息
 * @author caobaoli
 */
@SuppressWarnings({"unused"})
public class DirectedAcyclicGraph<Node, NodeInfo, EdgeInfo> {

    /**
     *
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * 顶点集合， Node为key， NodeInfo为value
     */
    private final Map<Node, NodeInfo> nodesMap = new HashMap<>();

    /**
     * 有向边集合，起点Node为key， 终点和指向终点的边组合的边组合的MAP为value
     */
    private final Map<Node, Map<Node, EdgeInfo>> edgesMap = new HashMap<>();

    /**
     * 反向边集合，终点Node为key，起点和背向起点的边组合为value
     */
    private final Map<Node, Map<Node, EdgeInfo>> reverseEdgesMap = new HashMap<>();


    public DirectedAcyclicGraph() {
    }


    public ImmutableGraph<Node> toImmutable() {
        var edgesMap = new HashMap<Node, Set<Node>>();
        for (var entry : this.edgesMap.entrySet()) {
            edgesMap.put(entry.getKey(), entry.getValue().keySet());
        }
        var reverseEdgesMap = new HashMap<Node, Set<Node>>();
        for (var entry : this.reverseEdgesMap.entrySet()) {
            reverseEdgesMap.put(entry.getKey(), entry.getValue().keySet());
        }
        return new ImmutableGraph<>(nodesMap.keySet(), edgesMap, reverseEdgesMap);
    }

    /**
     * 添加Node
     *
     * @param node     顶点ID或者顶点key
     * @param nodeInfo 顶点信息
     */
    public void addNode(Node node, NodeInfo nodeInfo) {
        lock.writeLock().lock();

        try {
            nodesMap.put(node, nodeInfo);
        } finally {
            lock.writeLock().unlock();
        }

    }


    /**
     * 添加有向边
     *
     * @param fromNode 起点
     * @param toNode   终点
     * @return 在图中添加一个有向边，如果dag图检测到由环则返回false
     */
    public boolean addEdge(Node fromNode, Node toNode) {
        return addEdge(fromNode, toNode, null, false);
    }


    /**
     * 添加有向边
     *
     * @param fromNode   起点
     * @param toNode     终点
     * @param createNode 当顶点不存在时是否要创建
     * @return 在图中添加一个有向边，如果dag图检测到由环则返回false
     */
    public boolean addEdge(Node fromNode, Node toNode, boolean createNode) {
        return addEdge(fromNode, toNode, null, createNode);
    }

    /**
     * @see #addEdge(Object, Object, Object, boolean)
     */
    public boolean addEdge(Node fromNode, Node toNode, EdgeInfo edge) {
        return addEdge(fromNode, toNode, edge, false);
    }

    /**
     * 添加有向边
     *
     * @param fromNode   起点
     * @param toNode     终点
     * @param edge       边
     * @param createNode 当顶点不存在时是否要创建
     * @return 在图中添加一个有向边，如果dag图检测到由环则返回false
     */
    public boolean addEdge(Node fromNode, Node toNode, EdgeInfo edge, boolean createNode) {
        lock.writeLock().lock();

        try {

            // 如果有向边添加成功 added(fromNode -> toNode)
            if (!isLegalAddEdge(fromNode, toNode, createNode)) {
                return false;
            }

            addNodeIfAbsent(fromNode, null);
            addNodeIfAbsent(toNode, null);

            addEdge(fromNode, toNode, edge, edgesMap);
            addEdge(toNode, fromNode, edge, reverseEdgesMap);

            return true;
        } finally {
            lock.writeLock().unlock();
        }

    }


    /**
     * 判断顶点集合中是否包含该顶点
     *
     * @param node node
     * @return 返回值为true标识包含；返回值为false为不包含
     */
    public boolean containsNode(Node node) {
        lock.readLock().lock();

        try {
            return nodesMap.containsKey(node);
        } finally {
            lock.readLock().unlock();
        }
    }


    /**
     * 判断边集合是否包含该边
     *
     * @param fromNode 起点
     * @param toNode   终点
     * @return 返回值为true标识包含；返回值为false为不包含
     */
    public boolean containsEdge(Node fromNode, Node toNode) {
        lock.readLock().lock();
        try {
            Map<Node, EdgeInfo> endEdges = edgesMap.get(fromNode);
            if (endEdges == null) {
                return false;
            }

            return endEdges.containsKey(toNode);
        } finally {
            lock.readLock().unlock();
        }
    }


    /**
     * 获取顶点对应的描述信息<NodeInfo>的值
     *
     * @param node 当前顶点
     * @return nodeInfo 顶点描述信息
     */
    public NodeInfo getNode(Node node) {
        lock.readLock().lock();

        try {
            return nodesMap.get(node);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 获取顶点对应的描述信息<NodeInfo>的值
     *
     * @param nodes 顶点集合
     * @return nodeInfo 顶点描述信息
     */
    public List<NodeInfo> getNodes(Collection<Node> nodes) {
        lock.readLock().lock();

        try {
            var li = new ArrayList<NodeInfo>(nodes.size());
            for (Node node : nodes) {
                var nodeInfo = nodesMap.get(node);
                if (null == nodeInfo) {
                    throw new IllegalArgumentException("node not exists: " + node);
                }
                li.add(nodeInfo);
            }
            return li;
        } finally {
            lock.readLock().unlock();
        }
    }


    public List<EdgeInfo> getEdges() {
        if (edgesMap.isEmpty()) {
            return List.of();
        }

        Set<EdgeInfo> resultSet = new HashSet<>();
        for (var entry : edgesMap.entrySet()) {
            resultSet.addAll(entry.getValue().values());
        }
        return List.copyOf(resultSet);

    }


    /**
     * 统计顶点个数
     *
     * @return 顶点个数
     */
    public int getNodesCount() {
        lock.readLock().lock();

        try {
            return nodesMap.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 统计有向边的个数
     *
     * @return 有向边的个数
     */
    public int getEdgesCount() {
        lock.readLock().lock();
        try {
            int count = 0;

            for (Map.Entry<Node, Map<Node, EdgeInfo>> entry : edgesMap.entrySet()) {
                count += entry.getValue().size();
            }

            return count;
        } finally {
            lock.readLock().unlock();
        }
    }


    /**
     * DAG图的起始节点也就是入度为0的顶点
     *
     * @return DAG图的起始节点集合
     */
    public Collection<Node> getBeginNode() {
        lock.readLock().lock();

        try {
            return DAGUtil.subtract(nodesMap.keySet(), reverseEdgesMap.keySet());
        } finally {
            lock.readLock().unlock();
        }

    }


    /**
     * 获取DAG图的终点，也就是出度为0
     *
     * @return dag图中出度为0的顶点
     */
    public Collection<Node> getEndNode() {

        lock.readLock().lock();

        try {
            return DAGUtil.subtract(nodesMap.keySet(), edgesMap.keySet());
        } finally {
            lock.readLock().unlock();
        }

    }

    /**
     * 获取当前顶点的所有先序顶点
     *
     * @param node 当前顶点
     * @return 先序顶点set集合
     */
    public Set<Node> getPreviousNodes(Node node) {
        lock.readLock().lock();
        try {
            return getNeighborNodes(node, reverseEdgesMap);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 获取当前顶点的所有先序顶点
     *
     * @param node 当前顶点
     * @return 先序顶点和边
     */
    public Map<Node, EdgeInfo> getPrevious(Node node) {
        lock.readLock().lock();
        try {
            return getNeighbors(node, reverseEdgesMap);
        } finally {
            lock.readLock().unlock();
        }
    }


    /**
     * 获取当前顶点的所有后继顶点
     *
     * @param node 当前顶点
     * @return 后继顶点set集合
     */
    public Set<Node> getSubsequentNodes(Node node) {
        lock.readLock().lock();

        try {
            return getNeighborNodes(node, edgesMap);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 获取当前顶点的所有后继顶点
     *
     * @param node 当前顶点
     * @return 后继顶点和边
     */
    public Map<Node, EdgeInfo> getSubsequent(Node node) {
        lock.readLock().lock();

        try {
            return getNeighbors(node, edgesMap);
        } finally {
            lock.readLock().unlock();
        }
    }


    /**
     * 获取当前顶点的入度
     *
     * @param node 当前顶点
     * @return 当前顶点的入度
     */
    public int getIndegree(Node node) {
        lock.readLock().lock();

        try {
            return getPreviousNodes(node).size();
        } finally {
            lock.readLock().unlock();
        }
    }


    /**
     * 判断图有没有环
     *
     * @return 返回true表示有环，否则为false
     */
    public boolean hasCycle() {
        lock.readLock().lock();
        try {
            return !topologicalSortImpl().getKey();
        } finally {
            lock.readLock().unlock();
        }
    }


    /**
     * 拓扑图按最短路径排序
     *
     * @return 排过序的拓扑图
     * @throws Exception errors
     */
    public List<Node> topologicalSort() throws Exception {
        lock.readLock().lock();

        try {
            Map.Entry<Boolean, List<Node>> entry = topologicalSortImpl();

            if (entry.getKey()) {
                return entry.getValue();
            }

            throw new IllegalStateException(" graph has cycle ! ");
        } finally {
            lock.readLock().unlock();
        }
    }


    /**
     * 如果检测到需要添加的顶点在graph中不存在，则添加
     *
     * @param node     顶点
     * @param nodeInfo 顶点信息
     */
    private void addNodeIfAbsent(Node node, NodeInfo nodeInfo) {
        nodesMap.putIfAbsent(node, nodeInfo);
    }


    /**
     * 添加有向边
     *
     * @param fromNode 起点
     * @param toNode   终点
     * @param edge     有向边
     * @param edges    有向边集合
     */
    private void addEdge(Node fromNode, Node toNode, EdgeInfo edge, Map<Node, Map<Node, EdgeInfo>> edges) {
        edges.computeIfAbsent(fromNode, (n) -> new HashMap<>()).put(toNode, edge);
    }


    /**
     * added(fromNode -> toNode) 如果有向边添加成功则需要检测这个DAG是否为环形图
     *
     * @param fromNode   起点
     * @param toNode     终点
     * @param createNode 是否需要创建顶点
     * @return true if added
     */
    private boolean isLegalAddEdge(Node fromNode, Node toNode, boolean createNode) {
        if (fromNode.equals(toNode)) {
            return false;
        }

        if (!createNode) {
            if (!containsNode(fromNode) || !containsNode(toNode)) {
                return false;
            }
        }

        int verticesCount = getNodesCount();

        Queue<Node> queue = new LinkedList<>();

        queue.add(toNode);

        while (!queue.isEmpty() && (--verticesCount > 0)) {
            Node key = queue.poll();

            for (Node subsequentNode : getSubsequentNodes(key)) {
                if (subsequentNode.equals(fromNode)) {
                    return false;
                }

                queue.add(subsequentNode);
            }
        }

        return true;
    }


    /**
     * 获取相关邻接顶点
     *
     * @param node  Node 当前顶点
     * @param edges 邻接边
     * @return 相关邻接顶点的集合
     */
    private Set<Node> getNeighborNodes(Node node, final Map<Node, Map<Node, EdgeInfo>> edges) {
        final Map<Node, EdgeInfo> neighborEdges = edges.get(node);

        if (neighborEdges == null) {
            return Set.of();
        }

        return Collections.unmodifiableSet(neighborEdges.keySet());
    }

    /**
     * 获取相关邻接顶点
     *
     * @param node  Node 当前顶点
     * @param edges 邻接边
     * @return 相关邻接顶点的集合
     */
    private Map<Node, EdgeInfo> getNeighbors(Node node, final Map<Node, Map<Node, EdgeInfo>> edges) {
        final Map<Node, EdgeInfo> neighborEdges = edges.get(node);

        if (neighborEdges == null) {
            return Map.of();
        }

        return Collections.unmodifiableMap(neighborEdges);
    }


    /**
     * <h2>确定是否有环和拓扑排序结果</h2>
     * <p>
     * 有向无环图(DAG)具有拓扑排序广度优先搜索：
     * <ol>
     *     <li>遍历图中所有顶点，进入度为0的顶点进入队列</li>
     *     <li>轮询队列中的一个顶点来更新它的邻接(- 1)，如果它在- 1之后为0，则对邻接进行排队</li>
     *     <li>循环执行step 2 直至队列为空</li>
     * </ol>
     * 如果不能遍历所有顶点，则意味着当前图不是有向无环图，不会对其做拓扑排序
     * </p>
     */
    private Map.Entry<Boolean, List<Node>> topologicalSortImpl() {
        // 用于存储零入度的队列
        Queue<Node> zeroInDegreeNodeQueue = new LinkedList<>();
        // 保存最终的结果
        List<Node> resultList = new ArrayList<>();
        // 保存非零入度的顶点
        Map<Node, Integer> nonzeroInDegreeNodeMap = new HashMap<>();

        for (Map.Entry<Node, NodeInfo> vertices : nodesMap.entrySet()) {
            Node node = vertices.getKey();
            int inDegree = getIndegree(node);

            if (inDegree == 0) {
                zeroInDegreeNodeQueue.add(node);
                resultList.add(node);
            } else {
                nonzeroInDegreeNodeMap.put(node, inDegree);
            }
        }

        /*
         * 0入度代表起始点，如果没有0入度就代表有环，直接退出
         */
        if (zeroInDegreeNodeQueue.isEmpty()) {
            return Map.entry(false, resultList);
        }

        while (!zeroInDegreeNodeQueue.isEmpty()) {
            Node v = zeroInDegreeNodeQueue.poll();

            Set<Node> subsequentNodes = getSubsequentNodes(v);

            for (Node subsequentNode : subsequentNodes) {

                Integer degree = nonzeroInDegreeNodeMap.get(subsequentNode);

                degree = degree - 1;//注意这里层层剥皮
                if (degree == 0) {
                    resultList.add(subsequentNode);
                    zeroInDegreeNodeQueue.add(subsequentNode);
                    nonzeroInDegreeNodeMap.remove(subsequentNode);
                } else {
                    nonzeroInDegreeNodeMap.put(subsequentNode, degree);
                }

            }
        }

        // 直至notZeroInDegreeNodeMap为空
        return Map.entry(nonzeroInDegreeNodeMap.size() == 0, resultList);

    }

}

