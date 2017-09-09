package graphast.gateway;

import java.io.IOException;
import java.util.HashMap;

import br.ufc.arida.analysis.algortihms.LinkScan;
import br.ufc.arida.analysis.model.ProbabilisticGraph;
import br.ufc.arida.analysis.model.cost.GaussianParser;
import br.ufc.arida.analysis.model.measures.KolnogorovSmirnovDistance;
import br.ufc.arida.dao.ProbabilisticCostsDAO;
import py4j.GatewayServer;

public class GraphastGateway {

	private HashMap<String, ProbabilisticGraph> graphs;
	private String costTable;

	public GraphastGateway() {
		graphs = new HashMap<String, ProbabilisticGraph>();
	}
	
	public void setTable(String tableName) {
		costTable = tableName;
	}

	public ProbabilisticGraph getGraph(String graphName) {
		return graphs.get(graphName);
	}
	
	public void pushGraph(String graphName, String path) {
		//"/Users/liviaalmada/git/traveltime/travel-time/resources/fortaleza-graphast"
		ProbabilisticGraph graph = new ProbabilisticGraph(path, new GaussianParser());
		graph.load();
		graphs.put(graphName, graph);
		System.out.println("Push graph "+graphName);
		
	}
	
	public void addGaussianCosts(String graphName, String graphhpMapFile, String table, int intervalsNum) {
		// "/Users/liviaalmada/git/traveltime/travel-time/resources/graphast-to-graphhopper-map"
		ProbabilisticCostsDAO dao = new ProbabilisticCostsDAO(graphhpMapFile, table);
		try {
			dao.addGaussianCost((ProbabilisticGraph) getGraph(graphName), false, intervalsNum);
			System.out.println("Done!");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void runLinkScanWithKolnogorovSmirnovDistance(String graphName, String clusterFileName, double epsSim, int epsNet, int minPoints, int time) {
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
