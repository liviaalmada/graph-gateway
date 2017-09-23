package graphast.gateway;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.graphast.geometry.Point;
import org.graphast.model.Edge;
import org.graphast.model.EdgeImpl;
import org.graphast.model.NodeImpl;
import org.graphast.util.DistanceUtils;

import br.ufc.arida.analysis.algortihms.LinkScan;
import br.ufc.arida.analysis.model.ProbabilisticGraph;
import br.ufc.arida.analysis.model.cost.GaussianCost;
import br.ufc.arida.analysis.model.cost.GaussianParser;
import br.ufc.arida.analysis.model.measures.KolnogorovSmirnovDistance;
import br.ufc.arida.dao.ProbabilisticCostsDAO;
import py4j.GatewayServer;

public class GraphastGateway {

	private HashMap<String, ProbabilisticGraph> graphs;
	private String costTable;
	public ProbabilisticGraph graph;

	public GraphastGateway() {
		graphs = new HashMap<String, ProbabilisticGraph>();
	}

	public void setTable(String tableName) {
		costTable = tableName;
	}

	public ProbabilisticGraph getGraph(String graphName) {
		graph = graphs.get(graphName);
		return graph;
	}

	public void newGraph(String name, String path) {
		ProbabilisticGraph graph = new ProbabilisticGraph(path, new GaussianParser());
		graphs.put(name, graph);
	}

	public void addNode(long exId, double lat, double lon) {
		NodeImpl n = new NodeImpl(exId, lat, lon);
		graph.addNode(n);
		System.out.println("Add node: "+n);
	}

	public void addEdge(long idSrc, long idTgt, double streetLength) {
		EdgeImpl e = new EdgeImpl(idSrc, idTgt, (int) Math.round(streetLength));
		List<Point> points = new ArrayList<Point>();
		points.add(new Point(graph.getNode(idSrc).getLatitude(), graph.getNode(idSrc).getLongitude()));
		points.add(new Point(graph.getNode(idTgt).getLatitude(), graph.getNode(idTgt).getLongitude()));
		e.setGeometry(points);
		graph.addEdge(e);
		
		System.out.println("Add edge: "+e);
		System.out.println(graph.getEdge(e.getId()));
	}

	public void pushGraph(String graphName, String path) {
		// "/Users/liviaalmada/git/traveltime/travel-time/resources/fortaleza-graphast"
		ProbabilisticGraph graph = new ProbabilisticGraph(path, new GaussianParser());
		graph.load();
		graphs.put(graphName, graph);
		System.out.println("Push graph " + graphName);

	}
	
	public void addGaussianCost( int nodeFrom, int nodeTo, double mean, double sigma, int time){
		System.out.println(nodeFrom+" "+nodeTo+" "+mean+" "+sigma+" "+time);
		Edge edge = graph.getEdge(nodeFrom, nodeTo);
		GaussianCost gaussian = new GaussianCost(mean, sigma);		
		graph.addProbabilisticCost(edge.getId(), time, gaussian);
		System.out.println("Added cost "+graph.getProbabilisticCosts(edge.getId(),time ));
	}

	public void addGaussianCosts(String graphName, String graphhpMapFile, String table, int intervalsNum) {
		// "/Users/liviaalmada/git/traveltime/travel-time/resources/graphast-to-graphhopper-map"
		ProbabilisticCostsDAO dao = new ProbabilisticCostsDAO(graphhpMapFile);
		try {
			dao.addGaussianCost((ProbabilisticGraph) getGraph(graphName), false, intervalsNum);
			System.out.println("Done!");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void runLinkScanWithKolnogorovSmirnovDistance(String graphName, String clusterFileName, double epsSim,
			int epsNet, int minPoints, int time) {
		ProbabilisticGraph graph = getGraph(graphName);
		LinkScan alg = new LinkScan(graph, new KolnogorovSmirnovDistance());
		System.out.println("Processing graph ... " + graphName);
		try {
			alg.runAndSave(clusterFileName, epsSim, epsNet, minPoints, time);
			System.out.println("Done!");
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		GatewayServer gatewayServer = new GatewayServer(new GraphastGateway());
		gatewayServer.start();
		System.out.println("Gateway Server Started");
	}

}
