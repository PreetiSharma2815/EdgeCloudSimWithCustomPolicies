/*
 * Title:        EdgeCloudSim - Edge Orchestrator
 *
 * Description:
 * SampleEdgeOrchestrator offloads tasks to proper server
 * by considering WAN bandwidth and edge server utilization.
 * After the target server is decided, the least loaded VM is selected.
 * If the target server is a remote edge server, MAN is used.
 *
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 * Copyright (c) 2017, Bogazici University, Istanbul, Turkey
 */

package edu.boun.edgecloudsim.applications.sample_app6;

import edu.boun.edgecloudsim.cloud_server.CloudVM;
import edu.boun.edgecloudsim.core.SimManager;
import edu.boun.edgecloudsim.core.SimSettings;
import edu.boun.edgecloudsim.edge_client.CpuUtilizationModel_Custom;
import edu.boun.edgecloudsim.edge_client.Task;
import edu.boun.edgecloudsim.edge_orchestrator.EdgeOrchestrator;
import edu.boun.edgecloudsim.edge_server.EdgeVM;
import edu.boun.edgecloudsim.utils.Location;
import edu.boun.edgecloudsim.utils.SimUtils;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEvent;

import java.util.List;
import java.util.Arrays;
import java.util.Random;

public class SampleEdgeOrchestrator6 extends EdgeOrchestrator {

	private int numberOfHost; //used by load balancer
	private int lastSelectedHostIndex; //used by load balancer
	private int[] lastSelectedVmIndexes; //used by each host individually

	public SampleEdgeOrchestrator6(String _policy, String _simScenario) {
		super(_policy, _simScenario);
	}

	@Override
	public void initialize() {
		numberOfHost=SimSettings.getInstance().getNumOfEdgeHosts();

		lastSelectedHostIndex = -1;
		lastSelectedVmIndexes = new int[numberOfHost];
		for(int i=0; i<numberOfHost; i++)
			lastSelectedVmIndexes[i] = -1;
	}

	@Override
	public int getDeviceToOffload(Task task) {
		int result = SimSettings.GENERIC_EDGE_DEVICE_ID;
		if(!simScenario.equals("SINGLE_TIER")){
			//decide to use cloud or Edge VM
			int CloudVmPicker = SimUtils.getRandomNumber(0, 100);

			if(CloudVmPicker <= SimSettings.getInstance().getTaskLookUpTable()[task.getTaskType()][1])
				result = SimSettings.CLOUD_DATACENTER_ID;
			else
				result = SimSettings.GENERIC_EDGE_DEVICE_ID;
		}

		return result;
	}

	@Override
	public Vm getVmToOffload(Task task, int deviceId) {
		Vm selectedVM = null;

		if(deviceId == SimSettings.CLOUD_DATACENTER_ID){
			//Select VM on cloud devices via Least Loaded algorithm!
			double selectedVmCapacity = 0; //start with min value
			List<Host> list = SimManager.getInstance().getCloudServerManager().getDatacenter().getHostList();
			for (int hostIndex=0; hostIndex < list.size(); hostIndex++) {
				List<CloudVM> vmArray = SimManager.getInstance().getCloudServerManager().getVmList(hostIndex);
				for(int vmIndex=0; vmIndex<vmArray.size(); vmIndex++){
					double requiredCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(vmArray.get(vmIndex).getVmType());
					double targetVmCapacity = (double)100 - vmArray.get(vmIndex).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());
					if(requiredCapacity <= targetVmCapacity && targetVmCapacity > selectedVmCapacity){
						selectedVM = vmArray.get(vmIndex);
						selectedVmCapacity = targetVmCapacity;
					}
				}
			}
		}
		else if(simScenario.equals("TWO_TIER_WITH_EO"))
			selectedVM = selectVmOnLoadBalancer(task);
		else
			selectedVM = selectVmOnHost(task);

		return selectedVM;
	}


	public EdgeVM selectVmOnHost(Task task){
		EdgeVM selectedVM = null;

		Location deviceLocation = SimManager.getInstance().getMobilityModel().getLocation(task.getMobileDeviceId(), CloudSim.clock());
		//in our scenasrio, serving wlan ID is equal to the host id
		//because there is only one host in one place
		int relatedHostId=deviceLocation.getServingWlanId();
		List<EdgeVM> vmArray = SimManager.getInstance().getEdgeServerManager().getVmList(relatedHostId);

		System.out.println("Related Host ID: " + relatedHostId);
		System.out.println(vmArray);

		if(policy.equalsIgnoreCase("RANDOM_FIT")){
			int randomIndex = SimUtils.getRandomNumber(0, vmArray.size()-1);
			double requiredCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(vmArray.get(randomIndex).getVmType());
			double targetVmCapacity = (double)100 - vmArray.get(randomIndex).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());
			if(requiredCapacity <= targetVmCapacity)
				selectedVM = vmArray.get(randomIndex);
		}
		else if(policy.equalsIgnoreCase("WORST_FIT")){
			double selectedVmCapacity = 0; //start with min value
			for(int vmIndex=0; vmIndex<vmArray.size(); vmIndex++){
				double requiredCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(vmArray.get(vmIndex).getVmType());
				double targetVmCapacity = (double)100 - vmArray.get(vmIndex).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());
				if(requiredCapacity <= targetVmCapacity && targetVmCapacity > selectedVmCapacity){
					selectedVM = vmArray.get(vmIndex);
					selectedVmCapacity = targetVmCapacity;
				}
			}
		}
		else if(policy.equalsIgnoreCase("BEST_FIT")){
			double selectedVmCapacity = 101; //start with max value
			for(int vmIndex=0; vmIndex<vmArray.size(); vmIndex++){
				double requiredCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(vmArray.get(vmIndex).getVmType());
				double targetVmCapacity = (double)100 - vmArray.get(vmIndex).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());
				if(requiredCapacity <= targetVmCapacity && targetVmCapacity < selectedVmCapacity){
					selectedVM = vmArray.get(vmIndex);
					selectedVmCapacity = targetVmCapacity;
				}
			}
		}
		else if(policy.equalsIgnoreCase("FIRST_FIT")){
			for(int vmIndex=0; vmIndex<vmArray.size(); vmIndex++){
				double requiredCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(vmArray.get(vmIndex).getVmType());
				double targetVmCapacity = (double)100 - vmArray.get(vmIndex).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());
				if(requiredCapacity <= targetVmCapacity){
					selectedVM = vmArray.get(vmIndex);
					break;
				}
			}
		}
		else if(policy.equalsIgnoreCase("NEXT_FIT")){
			int tries = 0;
			while(tries < vmArray.size()){
				lastSelectedVmIndexes[relatedHostId] = (lastSelectedVmIndexes[relatedHostId]+1) % vmArray.size();
				double requiredCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(vmArray.get(lastSelectedVmIndexes[relatedHostId]).getVmType());
				double targetVmCapacity = (double)100 - vmArray.get(lastSelectedVmIndexes[relatedHostId]).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());
				if(requiredCapacity <= targetVmCapacity){
					selectedVM = vmArray.get(lastSelectedVmIndexes[relatedHostId]);
					break;
				}
				tries++;
			}
		}
		else if (policy.equalsIgnoreCase("ROUND_ROBIN")) {
			// Get the last selected VM index for the current host
			int lastSelectedVmIndex = lastSelectedVmIndexes[relatedHostId];

			// Start from the next VM in the list (circular manner)
			int nextVmIndex = (lastSelectedVmIndex + 1) % vmArray.size();

			int tries = 0;
			while (tries < vmArray.size()) {
				// Get the VM at the next index
				EdgeVM nextVM = vmArray.get(nextVmIndex);

				// Calculate required and target capacity for the next VM
				double requiredCapacity = ((CpuUtilizationModel_Custom) task.getUtilizationModelCpu()).predictUtilization(nextVM.getVmType());
				double targetVmCapacity = 100 - nextVM.getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());

				// Check if the next VM has enough capacity to accommodate the task
				if (requiredCapacity <= targetVmCapacity) {
					selectedVM = nextVM;
					// Update the last selected VM index for the current host
					lastSelectedVmIndexes[relatedHostId] = nextVmIndex;
					break;
				}

				// Move to the next VM in the circular list
				nextVmIndex = (nextVmIndex + 1) % vmArray.size();
				tries++;
			}
		}
		else if (policy.equalsIgnoreCase("MAX_MIN")) {
			double maxAvailableCapacity = 0; // Start with min value
			for (int vmIndex = 0; vmIndex < vmArray.size(); vmIndex++) {
				double requiredCapacity = ((CpuUtilizationModel_Custom) task.getUtilizationModelCpu()).predictUtilization(vmArray.get(vmIndex).getVmType());
				double targetVmCapacity = 100 - vmArray.get(vmIndex).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());

				// Check if the VM can accommodate the task and has higher available capacity
				if (requiredCapacity <= targetVmCapacity && targetVmCapacity > maxAvailableCapacity) {
					selectedVM = vmArray.get(vmIndex);
					maxAvailableCapacity = targetVmCapacity;
				}
			}
		}
		else if (policy.equalsIgnoreCase("ANT_COLONY_OPTIMIZATION")) {
			// ACO-specific parameters
			int numAnts = 10; // Number of ants in the colony
			int maxIterations = 50; // Maximum number of iterations
			double alpha = 1.0; // Pheromone importance factor
			double beta = 2.0; // Heuristic information importance factor
			double evaporationRate = 0.1; // Pheromone evaporation rate
			double initialPheromone = 0.1; // Initial pheromone level

			// Initialize pheromone levels
			double[][] pheromones = new double[numAnts][vmArray.size()];
			for (int i = 0; i < numAnts; i++) {
				Arrays.fill(pheromones[i], initialPheromone);
			}
			try{

				double globalBestFitness = Double.POSITIVE_INFINITY;
				int[] globalBestSolution = new int[vmArray.size()];

				// ACO main loop
				for (int iteration = 0; iteration < maxIterations; iteration++) {
					int[] solutions = new int[numAnts];
					double[] fitnesses = new double[numAnts];

					// Construct solutions for each ant
					for (int antIndex = 0; antIndex < numAnts; antIndex++) {
						int selectedVMIndex = -1;
						double fitness = 0.0;

						try{
							selectedVMIndex = constructSolution(antIndex, pheromones, alpha, beta, vmArray);
							System.out.println("Selected VM Index by Ant: " + Double.toString(globalBestFitness));
							fitness = evaluateFitness(selectedVMIndex, vmArray, task); // Evaluate the fitness of the solution
						}
						catch (Exception e){
							System.out.println(e.getMessage());
						}


						solutions[antIndex] = selectedVMIndex; // Store selected VM index
						fitnesses[antIndex] = fitness; // Store fitness value
						System.out.println("Fitness: " + Double.toString(fitness));

						// Update global best if necessary
						if (fitness < globalBestFitness) {
							globalBestFitness = fitness;
							System.out.println("Global Fitness Updated: " + Double.toString(globalBestFitness));
							System.arraycopy(solutions, 0, globalBestSolution, 0, solutions.length);
						}
						System.out.println("Global Fitness: " + Double.toString(fitness));
					}

					// Update pheromone levels
					updatePheromones(pheromones, solutions, fitnesses, evaporationRate);
				}


				System.out.println("Global Best Solution: " + Arrays.toString(globalBestSolution));
				if (globalBestSolution != null) {
					selectedVM = vmArray.get(globalBestSolution[relatedHostId]);
				}

			}catch (ArrayIndexOutOfBoundsException e) {
				e.printStackTrace();
			}

		}

		return selectedVM;
	}

	private double evaluateFitness(int selectedVMIndex, List<EdgeVM> vmArray,Task task ) {
		// Check if selectedVMIndex is valid

		if (selectedVMIndex >= 0 && selectedVMIndex < vmArray.size()) {
			EdgeVM selectedVM = vmArray.get(selectedVMIndex);

			double vmMipsCapacity = selectedVM.getMips();
			// Get the task's processing requirements (in MIPS)
			double taskMipsRequirement = task.getCloudletTotalLength(); // This might need to be scaled or adjusted

			// Calculate the execution time in seconds
			double executionTimeInSeconds = taskMipsRequirement / vmMipsCapacity;

			// Convert execution time to milliseconds
			double executionTimeInMilliseconds = executionTimeInSeconds * 1000;
			double taskExecutionTime = executionTimeInMilliseconds;

			//double taskExecutionTime = selectedVM.getCloudletScheduler().getEstimatedFinishTime(CloudSim.clock());
			double totalUtilization = selectedVM.getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock()) / 100.0;
			double executionTimeWeight = 0.7; // Importance of minimizing execution time
			double utilizationWeight = 0.3; // Importance of minimizing resource utilization
			double fitness = executionTimeWeight * taskExecutionTime + utilizationWeight * totalUtilization;
			return fitness;
		}
		else {
			// Handle the case where selectedVMIndex is invalid
			return Double.POSITIVE_INFINITY; // Return a large value to indicate invalid index
		}
	}

	private int constructSolution(int antIndex, double[][] pheromones, double alpha, double beta, List<EdgeVM> vmArray) {
		int selectedVMIndex = -1;
		double totalProbabilities = 0.0;

		// Calculate probabilities for selecting each VM based on pheromone and heuristic information
		double[] probabilities = new double[vmArray.size()];
		for (int vmIndex = 0; vmIndex < vmArray.size(); vmIndex++) {
			double pheromoneFactor = Math.pow(pheromones[antIndex][vmIndex], alpha);
			double heuristicFactor = Math.pow(1.0 / vmArray.get(vmIndex).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock()), beta);

			System.out.println("pheromoneFactor:" + pheromoneFactor);
			System.out.println("heuristicFactor:" + heuristicFactor);

			probabilities[vmIndex] = pheromoneFactor * heuristicFactor;
			totalProbabilities += probabilities[vmIndex];

			System.out.println("probabilities:" + Arrays.toString(probabilities));
			System.out.println("totalProbabilities:" + totalProbabilities);
		}

		// Select a VM based on probabilities using a roulette wheel selection

		double randomValue = new Random().nextDouble(); // Generates a random double between 0 (inclusive) and 1 (exclusive)
//		double randomValue = SimUtils.getRandomNumber(0, vmArray.size()-1);
		System.out.println("Random Value"+ randomValue);
		double cumulativeProbability = 0.0;
		for (int vmIndex = 0; vmIndex < vmArray.size(); vmIndex++) {
			cumulativeProbability += probabilities[vmIndex] / totalProbabilities;
			System.out.println("Cummilative Prob:" + cumulativeProbability);
			if (randomValue <= cumulativeProbability) {
				selectedVMIndex = vmIndex;
				break;
			}
		}
		System.out.println("Selected VM Index returned:" + selectedVMIndex);
		return selectedVMIndex;
	}

	private void updatePheromones(double[][] pheromones, int[] solutions, double[] fitnesses, double evaporationRate) {
		for (int antIndex = 0; antIndex < solutions.length; antIndex++) {
			double pheromoneDelta = 1.0 / fitnesses[antIndex];
			for (int vmIndex = 0; vmIndex < pheromones[antIndex].length; vmIndex++) {
				pheromones[antIndex][vmIndex] = (1 - evaporationRate) * pheromones[antIndex][vmIndex] + pheromoneDelta;
			}
		}
	}

	public EdgeVM selectVmOnLoadBalancer(Task task){
		EdgeVM selectedVM = null;

		if(policy.equalsIgnoreCase("RANDOM_FIT")){
			int randomHostIndex = SimUtils.getRandomNumber(0, numberOfHost-1);
			List<EdgeVM> vmArray = SimManager.getInstance().getEdgeServerManager().getVmList(randomHostIndex);
			int randomIndex = SimUtils.getRandomNumber(0, vmArray.size()-1);

			double requiredCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(vmArray.get(randomIndex).getVmType());
			double targetVmCapacity = (double)100 - vmArray.get(randomIndex).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());
			if(requiredCapacity <= targetVmCapacity)
				selectedVM = vmArray.get(randomIndex);
		}
		else if(policy.equalsIgnoreCase("WORST_FIT")){
			double selectedVmCapacity = 0; //start with min value
			for(int hostIndex=0; hostIndex<numberOfHost; hostIndex++){
				List<EdgeVM> vmArray = SimManager.getInstance().getEdgeServerManager().getVmList(hostIndex);
				for(int vmIndex=0; vmIndex<vmArray.size(); vmIndex++){
					double requiredCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(vmArray.get(vmIndex).getVmType());
					double targetVmCapacity = (double)100 - vmArray.get(vmIndex).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());
					if(requiredCapacity <= targetVmCapacity && targetVmCapacity > selectedVmCapacity){
						selectedVM = vmArray.get(vmIndex);
						selectedVmCapacity = targetVmCapacity;
					}
				}
			}
		}
		else if(policy.equalsIgnoreCase("BEST_FIT")){
			double selectedVmCapacity = 101; //start with max value
			for(int hostIndex=0; hostIndex<numberOfHost; hostIndex++){
				List<EdgeVM> vmArray = SimManager.getInstance().getEdgeServerManager().getVmList(hostIndex);
				for(int vmIndex=0; vmIndex<vmArray.size(); vmIndex++){
					double requiredCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(vmArray.get(vmIndex).getVmType());
					double targetVmCapacity = (double)100 - vmArray.get(vmIndex).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());
					if(requiredCapacity <= targetVmCapacity && targetVmCapacity < selectedVmCapacity){
						selectedVM = vmArray.get(vmIndex);
						selectedVmCapacity = targetVmCapacity;
					}
				}
			}
		}
		else if(policy.equalsIgnoreCase("FIRST_FIT")){
			for(int hostIndex=0; hostIndex<numberOfHost; hostIndex++){
				List<EdgeVM> vmArray = SimManager.getInstance().getEdgeServerManager().getVmList(hostIndex);
				for(int vmIndex=0; vmIndex<vmArray.size(); vmIndex++){
					double requiredCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(vmArray.get(vmIndex).getVmType());
					double targetVmCapacity = (double)100 - vmArray.get(vmIndex).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());
					if(requiredCapacity <= targetVmCapacity){
						selectedVM = vmArray.get(vmIndex);
						break;
					}
				}
			}
		}
		else if(policy.equalsIgnoreCase("NEXT_FIT")){
			int hostCheckCounter = 0;
			while(selectedVM == null && hostCheckCounter < numberOfHost){
				int tries = 0;
				lastSelectedHostIndex = (lastSelectedHostIndex+1) % numberOfHost;

				List<EdgeVM> vmArray = SimManager.getInstance().getEdgeServerManager().getVmList(lastSelectedHostIndex);
				while(tries < vmArray.size()){
					lastSelectedVmIndexes[lastSelectedHostIndex] = (lastSelectedVmIndexes[lastSelectedHostIndex]+1) % vmArray.size();
					double requiredCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(vmArray.get(lastSelectedVmIndexes[lastSelectedHostIndex]).getVmType());
					double targetVmCapacity = (double)100 - vmArray.get(lastSelectedVmIndexes[lastSelectedHostIndex]).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());
					if(requiredCapacity <= targetVmCapacity){
						selectedVM = vmArray.get(lastSelectedVmIndexes[lastSelectedHostIndex]);
						break;
					}
					tries++;
				}

				hostCheckCounter++;
			}
		}

		return selectedVM;
	}

	@Override
	public void processEvent(SimEvent arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void shutdownEntity() {
		// TODO Auto-generated method stub

	}

	@Override
	public void startEntity() {
		// TODO Auto-generated method stub

	}

}