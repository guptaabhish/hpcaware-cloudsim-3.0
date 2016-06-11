/*
 * Title:        CloudSim Toolkit
 * Description:  CloudSim (Cloud Simulation) Toolkit for Modeling and Simulation of Clouds
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2009-2012, The University of Melbourne, Australia
 */

package org.cloudbus.cloudsim;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.lists.CloudletList;
import org.cloudbus.cloudsim.lists.VmList;

/**
 * DatacentreBroker represents a broker acting on behalf of a user. It hides VM management, as vm
 * creation, sumbission of cloudlets to this VMs and destruction of VMs.
 * 
 * @author Rodrigo N. Calheiros
 * @author Anton Beloglazov
 * @since CloudSim Toolkit 1.0
 */
public class DatacenterBroker extends SimEntity {

	/** The vm list. */
	protected List<? extends Vm> vmList;

	/** The vms created list. */
	protected List<? extends Vm> vmsCreatedList;

	/** The cloudlet list. */
	protected List<? extends Cloudlet> cloudletList;

	/** The cloudlet submitted list. */
	protected List<? extends Cloudlet> cloudletSubmittedList;

	/** The cloudlet received list. */
	protected List<? extends Cloudlet> cloudletReceivedList;

	/** The cloudlets submitted. */
	protected int cloudletsSubmitted;

	/** The vms requested. */
	protected int vmsRequested;

	/** The vms acks. */
	protected int vmsAcks;
 
public int POLICY = 0;

	/** The vms destroyed. */
	protected int vmsDestroyed;

	/** The datacenter ids list. */
	protected List<Integer> datacenterIdsList;

	/** The datacenter requested ids list. */
	protected List<Integer> datacenterRequestedIdsList;

	/** The vms to datacenters map. */
	protected Map<Integer, Integer> vmsToDatacentersMap;

	/** The datacenter characteristics list. */
	protected Map<Integer, DatacenterCharacteristics> datacenterCharacteristicsList;

// each row is an app, within that first each pe 1, 2, 4, 8, 16, 32, 64, 128, 256
	private double [][][] slowdownTable = {

{ { 0.916033,  0.881993}, {0.916308,  0.929236}, {0.925102, 0.901044}, {0.99184, 0.901179 }, {0.951087, 0.972826}, {0.985507, 1.22101}, {0.978261, 1.16667}, {1.33333,  0.956522}, {2.05714, 1.6}}, 

{{0.613848, 0.355239},
{0.494952, 0.425873},
{0.450863, 0.334441},
{0.746916, 0.639302},
{1.06181, 1.09343},
{0.917296, 1.61083},
{1.43536, 2.68692},
{1.80851, 5.00665},
{2.46875, 12.8341}},


{{0.742406, 0.501823},
{0.927907, 1.00233},
{1.01017, 2.54915},
{2.55975, 3.10063},
{4.9596, 6.52525},
{10.1875, 7.225},
{24.6923, 23.5},
{54.5556, 153.711},
{110.557, 467.508}},

{{0.567273, 0.558},
{0.630282, 0.570423},
{0.632653, 0.62585},
{0.74026, 0.883117},
{0.833333, 0.97619},
{0.904762, 1.2381},
{1.41414, 1.81818},
{1.52542, 2.0339},
{1.46341, 2.43902}},

{{0.495572, 0.476015},
{0.501105, 0.512159},
{0.507937, 0.558442},
{0.528571, 0.685714},
{0.819767, 1.09302},
{1.3908, 1.6092},
{1.62105, 2.52632},
{2.25926, 4.81481},
{3.46154, 22.4359}},

{{0.776753, 0.747923},
{0.782595, 0.798247},
{0.812077, 0.792296},
{0.714314, 0.753637},
{0.741493, 0.781337},
{0.733179, 0.764114},
{0.647292, 0.726552},
{0.592668, 0.674134},
{0.75, 0.946429}},

{{0.21494, 0.219518},
{0.254902, 0.269866},
{0.226355, 0.41339},
{0.392857, 0.546218},
{0.447154, 0.914634},
{0.570312, 1.59375},
{0.766234, 2.33766},
{0.982143, 3.30357},
{2.33333, 2.42222}},

{{0.857711, 0.490121},
{0.725959, 0.667882},
{0.588508, 2.5659},
{0.803498, 3.52646},
{1.0071, 5.37836},
{0.919887, 5.67853},
{0.703059, 4.3824},
{1.03367, 6.88586},
{7.19821, 6.3507}},
	};



//	private int [][][] slowdownTable = new int [6][9][2];

/* { {{1, 1, 1, 1, 1, 1, 1, 1, 1 }, {2, 2, 2, 2, 2, 2, 2, 2, 2} },  
  {{1, 1, 1, 1, 1, 1, 1, 1, 1 }, {2, 2, 2, 2, 2, 2, 2, 2, 2} },   
 {{1, 1, 1, 1, 1, 1, 1, 1, 1 }, {2, 2, 2, 2, 2, 2, 2, 2, 2} },  
  {{1, 1, 1, 1, 1, 1, 1, 1, 1 }, {2, 2, 2, 2, 2, 2, 2, 2, 2} }, 
   {{1, 1, 1, 1, 1, 1, 1, 1, 1 }, {2, 2, 2, 2, 2, 2, 2, 2, 2} },  
  {{1, 1, 1, 1, 1, 1, 1, 1, 1 }, {2, 2, 2, 2, 2, 2, 2, 2, 2} },                                                       	  
	};
*/

	/**
	 * Created a new DatacenterBroker object.
	 * 
	 * @param name name to be associated with this entity (as required by Sim_entity class from
	 *            simjava package)
	 * @throws Exception the exception
	 * @pre name != null
	 * @post $none
	 */
	public DatacenterBroker(String name) throws Exception {
		super(name);

		setVmList(new ArrayList<Vm>());
		setVmsCreatedList(new ArrayList<Vm>());
		setCloudletList(new ArrayList<Cloudlet>());
		setCloudletSubmittedList(new ArrayList<Cloudlet>());
		setCloudletReceivedList(new ArrayList<Cloudlet>());

		cloudletsSubmitted = 0;
		setVmsRequested(0);
		setVmsAcks(0);
		setVmsDestroyed(0);

		setDatacenterIdsList(new LinkedList<Integer>());
		setDatacenterRequestedIdsList(new ArrayList<Integer>());
		setVmsToDatacentersMap(new HashMap<Integer, Integer>());
		setDatacenterCharacteristicsList(new HashMap<Integer, DatacenterCharacteristics>());
//cluster
	/*	slowdownTable[0][0][0] = 1;
		slowdownTable[0][1][0] = 1;
		slowdownTable[0][2][0] = 1;
		slowdownTable[0][3][0] = 1;
		slowdownTable[0][4][0] = 1;
		slowdownTable[0][5][0] = 1;
		slowdownTable[0][6][0] = 1;
		slowdownTable[0][7][0] = 1;
		slowdownTable[0][8][0] = 1;

		slowdownTable[0][0][1] = 2;
		slowdownTable[0][1][1] = 2;
		slowdownTable[0][2][1] = 2;
		slowdownTable[0][3][1] = 2;
		slowdownTable[0][4][1] = 2;
		slowdownTable[0][5][1] = 2;
		slowdownTable[0][6][1] = 2;
		slowdownTable[0][7][1] = 2;
		slowdownTable[0][8][1] = 2;

		slowdownTable[1][0][0] = 1;
		slowdownTable[1][1][0] = 1;
		slowdownTable[1][2][0] = 1;
		slowdownTable[1][3][0] = 1;
		slowdownTable[1][4][0] = 1;
		slowdownTable[1][5][0] = 1;
		slowdownTable[1][6][0] = 1;
		slowdownTable[1][7][0] = 1;
		slowdownTable[1][8][0] = 1;

		slowdownTable[1][0][1] = 2;
		slowdownTable[1][1][1] = 2;
		slowdownTable[1][2][1] = 2;
		slowdownTable[1][3][1] = 2;
		slowdownTable[1][4][1] = 2;
		slowdownTable[1][5][1] = 2;
		slowdownTable[1][6][1] = 2;
		slowdownTable[1][7][1] = 2;
		slowdownTable[1][8][1] = 2;

		slowdownTable[2][0][0] = 1;
		slowdownTable[2][1][0] = 1;
		slowdownTable[2][2][0] = 1;
		slowdownTable[2][3][0] = 1;
		slowdownTable[2][4][0] = 1;
		slowdownTable[2][5][0] = 1;
		slowdownTable[2][6][0] = 1;
		slowdownTable[2][7][0] = 1;
		slowdownTable[2][8][0] = 1;

		slowdownTable[2][0][1] = 2;
		slowdownTable[2][1][1] = 2;
		slowdownTable[2][2][1] = 2;
		slowdownTable[2][3][1] = 2;
		slowdownTable[2][4][1] = 2;
		slowdownTable[2][5][1] = 2;
		slowdownTable[2][6][1] = 2;
		slowdownTable[2][7][1] = 2;
		slowdownTable[2][8][1] = 2;

		slowdownTable[3][0][0] = 1;
		slowdownTable[3][1][0] = 1;
		slowdownTable[3][2][0] = 1;
		slowdownTable[3][3][0] = 1;
		slowdownTable[3][4][0] = 1;
		slowdownTable[3][5][0] = 1;
		slowdownTable[3][6][0] = 1;
		slowdownTable[3][7][0] = 1;
		slowdownTable[3][8][0] = 1;

		slowdownTable[3][0][1] = 2;
		slowdownTable[3][1][1] = 2;
		slowdownTable[3][2][1] = 2;
		slowdownTable[3][3][1] = 2;
		slowdownTable[3][4][1] = 2;
		slowdownTable[3][5][1] = 2;
		slowdownTable[3][6][1] = 2;
		slowdownTable[3][7][1] = 2;
		slowdownTable[3][8][1] = 2;

		slowdownTable[4][0][0] = 1;
		slowdownTable[4][1][0] = 1;
		slowdownTable[4][2][0] = 1;
		slowdownTable[4][3][0] = 1;
		slowdownTable[4][4][0] = 1;
		slowdownTable[4][5][0] = 1;
		slowdownTable[4][6][0] = 1;
		slowdownTable[4][7][0] = 1;
		slowdownTable[4][8][0] = 1;

		slowdownTable[4][0][1] = 2;
		slowdownTable[4][1][1] = 2;
		slowdownTable[4][2][1] = 2;
		slowdownTable[4][3][1] = 2;
		slowdownTable[4][4][1] = 2;
		slowdownTable[4][5][1] = 2;
		slowdownTable[4][6][1] = 2;
		slowdownTable[4][7][1] = 2;
		slowdownTable[4][8][1] = 2;


		slowdownTable[5][0][0] = 1;
		slowdownTable[5][1][0] = 1;
		slowdownTable[5][2][0] = 1;
		slowdownTable[5][3][0] = 1;
		slowdownTable[5][4][0] = 1;
		slowdownTable[5][5][0] = 1;
		slowdownTable[5][6][0] = 1;
		slowdownTable[5][7][0] = 1;
		slowdownTable[5][8][0] = 1;

		slowdownTable[5][0][1] = 2;
		slowdownTable[5][1][1] = 2;
		slowdownTable[5][2][1] = 2;
		slowdownTable[5][3][1] = 2;
		slowdownTable[5][4][1] = 2;
		slowdownTable[5][5][1] = 2;
		slowdownTable[5][6][1] = 2;
		slowdownTable[5][7][1] = 2;
		slowdownTable[5][8][1] = 2;
*/
	}

	/**
	 * This method is used to send to the broker the list with virtual machines that must be
	 * created.
	 * 
	 * @param list the list
	 * @pre list !=null
	 * @post $none
	 */
	public void submitVmList(List<? extends Vm> list) {
		getVmList().addAll(list);
	}

	/**
	 * This method is used to send to the broker the list of cloudlets.
	 * 
	 * @param list the list
	 * @pre list !=null
	 * @post $none
	 */
	public void submitCloudletList(List<? extends Cloudlet> list) {
		getCloudletList().addAll(list);
	}

	/**
	 * Specifies that a given cloudlet must run in a specific virtual machine.
	 * 
	 * @param cloudletId ID of the cloudlet being bount to a vm
	 * @param vmId the vm id
	 * @pre cloudletId > 0
	 * @pre id > 0
	 * @post $none
	 */
	public void bindCloudletToVm(int cloudletId, int vmId) {
		CloudletList.getById(getCloudletList(), cloudletId).setVmId(vmId);
	}

	/**
	 * Processes events available for this Broker.
	 * 
	 * @param ev a SimEvent object
	 * @pre ev != null
	 * @post $none
	 */
	@Override
	public void processEvent(SimEvent ev) {
		switch (ev.getTag()) {
		// Resource characteristics request
			case CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST:
				processResourceCharacteristicsRequest(ev);
				break;
			// Resource characteristics answer
			case CloudSimTags.RESOURCE_CHARACTERISTICS:
				processResourceCharacteristics(ev);
				break;
			// VM Creation answer
			case CloudSimTags.VM_CREATE_ACK:
				processVmCreate(ev);
				break;
			// A finished cloudlet returned
			case CloudSimTags.CLOUDLET_RETURN:
				processCloudletReturn(ev);
				break;
			// if the simulation finishes
			case CloudSimTags.END_OF_SIMULATION:
				shutdownEntity();
				break;
			// other unknown tags are processed by this method
			default:
				processOtherEvent(ev);
				break;
		}
	}

	/**
	 * Process the return of a request for the characteristics of a PowerDatacenter.
	 * 
	 * @param ev a SimEvent object
	 * @pre ev != $null
	 * @post $none
	 */
	protected void processResourceCharacteristics(SimEvent ev) {
		DatacenterCharacteristics characteristics = (DatacenterCharacteristics) ev.getData();
		getDatacenterCharacteristicsList().put(characteristics.getId(), characteristics);

		if (getDatacenterCharacteristicsList().size() == getDatacenterIdsList().size()) {
			setDatacenterRequestedIdsList(new ArrayList<Integer>());
			createVmsInDatacenter(getDatacenterIdsList().get(0));
		}
	}

	/**
	 * Process a request for the characteristics of a PowerDatacenter.
	 * 
	 * @param ev a SimEvent object
	 * @pre ev != $null
	 * @post $none
	 */
	protected void processResourceCharacteristicsRequest(SimEvent ev) {
		setDatacenterIdsList(CloudSim.getCloudResourceList());
		setDatacenterCharacteristicsList(new HashMap<Integer, DatacenterCharacteristics>());

		Log.printLine(CloudSim.clock() + ": " + getName() + ": Cloud Resource List received with "
				+ getDatacenterIdsList().size() + " resource(s)");

		for (Integer datacenterId : getDatacenterIdsList()) {
			sendNow(datacenterId, CloudSimTags.RESOURCE_CHARACTERISTICS, getId());
		}
		Log.printLine(CloudSim.clock() + ": " + getName() + ":Sending periodic timer ");
		send(getId(),0.5,CloudSimTags.PERIODIC_EVENT,getId());

	}

	/**
	 * Process the ack received due to a request for VM creation.
	 * 
	 * @param ev a SimEvent object
	 * @pre ev != null
	 * @post $none
	 */
	protected void processVmCreate(SimEvent ev) {
		int[] data = (int[]) ev.getData();
		int datacenterId = data[0];
//	Log.printLine("Inside process VM ");
		int vmId = data[1];
		int result = data[2];
//	Log.printLine("Inside process VM ");
	//		Log.printLine(CloudSim.clock() + ": " + getName() + ": VM #" + vmId
	//				+ " has been created in Datacenter #" + datacenterId + ", Host #"
	//				+ VmList.getById(getVmsCreatedList(), vmId).getHost().getId());
//	Log.printLine("Inside process VM ");
		if (result == CloudSimTags.TRUE) {
//		Log.printLine(CloudSim.clock() + ": " + getName() + ": Creation of VM #" + vmId
//					+ " Succeeded in Datacenter #" + datacenterId);
//UNCOMMENT THIS 	getVmsToDatacentersMap().put(vmId, datacenterId);
			getVmsCreatedList().add(VmList.getById(getVmList(), vmId));
	//		Log.printLine(CloudSim.clock() + ": " + getName() + ": VM #" + vmId
	//				+ " has been created in Datacenter #" + datacenterId + ", Host #"
	//				+ VmList.getById(getVmsCreatedList(), vmId).getHost().getId());
		} else {
		//	Log.printLine(CloudSim.clock() + ": " + getName() + ": Creation of VM #" + vmId
		//			+ " failed in Datacenter #" + datacenterId);
		}

		incrementVmsAcks();

		submitCloudlets();
		// all the requested VMs have been created
		if (getVmsCreatedList().size() == getVmList().size() - getVmsDestroyed()) {
	//		submitCloudlets();
		} else {
			// all the acks received, but some VMs were not created
			if (getVmsRequested() == getVmsAcks()) {
				// find id of the next datacenter that has not been tried
				for (int nextDatacenterId : getDatacenterIdsList()) {
					if (!getDatacenterRequestedIdsList().contains(nextDatacenterId)) {
						createVmsInDatacenter(nextDatacenterId);
					//	return;
					}
				}

				// all datacenters already queried
				if (getVmsCreatedList().size() > 0) { // if some vm were created
	//				submitCloudlets();
				} else { // no vms created. abort
					Log.printLine(CloudSim.clock() + ": " + getName()
							+ ": none of the required VMs could be created. Aborting");
					finishExecution();
				}
			}
		}
	}

	/**
	 * Process a cloudlet return event.
	 * 
	 * @param ev a SimEvent object
	 * @pre ev != $null
	 * @post $none
	 */
	protected void processCloudletReturn(SimEvent ev) {
		Cloudlet cloudlet = (Cloudlet) ev.getData();
		getCloudletReceivedList().add(cloudlet);
		Log.printLine(CloudSim.clock() + ": " + getName() + ": Cloudlet " + cloudlet.getCloudletId()
				+ " received");
		cloudletsSubmitted--;

// Destroy VM here
  int vmIndex=0;
  		for (Vm vm : getVmsCreatedList()) {
			if (vm.getId()==cloudlet.getVmId()){
					Log.printLine(CloudSim.clock() + ": " + getName() + ": Destroying VM #" + vm.getId());
					break;
		}
		 vmIndex++;
		}
	Vm vm = getVmsCreatedList().get(vmIndex);
	Log.printLine(CloudSim.clock() + ": " + getName() + ": Destroying VM #" + vm.getId());
	sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.VM_DESTROY, vm);
	getVmsCreatedList().remove(vmIndex);
		getVmList().remove(vm);

//see if new VMs can be added
					//if (!getDatacenterRequestedIdsList().contains(nextDatacenterId)) 
					{
					Log.printLine(CloudSim.clock() + ": " + getName() + ": Destroying VM #" + vm.getId());
					createVmsInDatacenter(getVmsToDatacentersMap().get(vm.getId()));
					}




		if (getCloudletList().size() == 0 && cloudletsSubmitted == 0) { // all cloudlets executed
			Log.printLine(CloudSim.clock() + ": " + getName() + ": All Cloudlets executed. Finishing...");
			clearDatacenters();
			finishExecution();
		} else { // some cloudlets haven't finished yet
			if (getCloudletList().size() > 0 && cloudletsSubmitted == 0) {
				// all the cloudlets sent finished. It means that some bount
				// cloudlet is waiting its VM be created
				clearDatacenters();
				createVmsInDatacenter(0);
			}

		}
	}


 private void processPeriodicEvent(SimEvent ev) {
    //your code here
     // invoke job to DC map here based on arrival time
     // fix VM policy fcfs tag
     // fix the 
    // some mechanism for quitting?
  int [] flag = {0,0,0} ;
		double curTime = CloudSim.clock();
// based on cloudlet list arrival times amd dc availability   
//	Log.printLine(getName() + "ProcessPeriodicEvent(): ");
   for (Cloudlet cloudlet : getCloudletList()) {
	double cloudletTime = cloudlet.cloudletArrivalTime;
//	Log.printLine(getName() + "ProcessPeriodicEvent(): " + curTime + "cloudlet time" + cloudletTime);
		if(cloudletTime <= curTime)	
		 { 
		//	Log.printLine(getName() + "Can start VM for this cloudlet now  " + curTime + "cloudlet time" + cloudletTime);
		//	vm = VmList.getById(getVmsCreatedList(), cloudlet.getVmId());
                   // set vm to DC map according to some algo
		if ( getVmsToDatacentersMap().get(cloudlet.getVmId()) ==null) { 
		//	Log.printLine(getName() + "Can start VM for this cloudlet, value was null now  " + curTime + "cloudlet time" + cloudletTime);
// queue to the DC with min free
	List <Integer> freepesList = new ArrayList<Integer>();
	for (Integer datacenterId : getDatacenterIdsList()) {
 		int freepes1 =((VmAllocationPolicySimple) ((Datacenter)CloudSim.getEntity(datacenterId)).getVmAllocationPolicy()). freeCount;
//	Log.printLine(getName() + "  **** DC record freepes " + datacenterId +" " +  freepes1);
		freepesList.add(freepes1); 
		}
	int appId = cloudlet.cloudletpe_cloud;
	Vm vm = VmList.getById(getVmList(), cloudlet.getVmId());
	int mypes = vm.getNumberOfPes();
	int peindex = 0;

	if(mypes == 1) peindex = 0;
	if(mypes == 2) peindex = 1;
	if(mypes >2 && mypes <=6) peindex = 2;
	if(mypes >6 && mypes <=12) peindex = 3;
	if(mypes >12 && mypes <=24) peindex = 4;
	if(mypes >24 && mypes <=48) peindex = 5;
	if(mypes == 64) peindex = 6;
	if(mypes == 128) peindex = 7;
	if(mypes == 256) peindex = 8;
	peindex +=3;

// ************************* Policy here 
	Log.printLine(getName() + "appid  " + appId  + "mype " + mypes+ "peindex" +peindex);
if (POLICY == 0) { // optimized policy
	int count =0;
	double maxEffFreePE =-1; 
	int maxIndex = 0;
	for (Integer freepes: freepesList) {
		if(count ==0) {maxEffFreePE = freepes; if(maxEffFreePE ==0) maxEffFreePE = 8;}
//else 	if(mypes <=16  &&  freepes/slowdownTable[appId][peindex][count-1] > maxEffFreePE) {
else 	if(mypes <=48  &&  freepes/slowdownTable[appId][peindex][count-1] > maxEffFreePE) {
                maxEffFreePE = freepes/slowdownTable[appId][peindex][count-1] ; 
                 maxIndex = count;
                   } 
if(count !=0)  	Log.printLine(getName() + " maxEffFreePE:  " + maxEffFreePE  + "count " +count + "slowdown was " + slowdownTable[appId][peindex][count-1]  );
		count ++;
		}
//	Log.printLine(getName() + " Will start VM for this cloudlet at  " + curTime + " on DC " + maxIndex+2);
	if (maxIndex !=0) {
		Log.printLine(getName() + ": Changing exec time from SC to  new dc   for vm "+ cloudlet.getVmId() +" from " +  cloudlet.getCloudletLength()  + " to "  + ((int)( cloudlet.getCloudletLength() * slowdownTable[appId][peindex][maxIndex-1]  )));
	 cloudlet.setCloudletLength ((int)(cloudlet.getCloudletLength() * slowdownTable[appId][peindex][maxIndex-1] ));
		}
// ****************** Policy ENDS
		getVmsToDatacentersMap().put(cloudlet.getVmId(), maxIndex+2);
	flag[maxIndex] =1;
                }
if (POLICY == 1) { // only SC policy
	int maxIndex = 0;
	getVmsToDatacentersMap().put(cloudlet.getVmId(), maxIndex+2);
	flag[maxIndex] =1;
}
if (POLICY == 2) { // only Cluster policy
	int maxIndex = 1;
	cloudlet.setCloudletLength ((int)(cloudlet.getCloudletLength() * slowdownTable[appId][peindex][maxIndex-1] ));
	getVmsToDatacentersMap().put(cloudlet.getVmId(), maxIndex+2);
	flag[maxIndex] =1;
}
if (POLICY == 3) { // only Cloud policy
	int maxIndex = 2;
	cloudlet.setCloudletLength ((int)(cloudlet.getCloudletLength() * slowdownTable[appId][peindex][maxIndex-1] ));
	getVmsToDatacentersMap().put(cloudlet.getVmId(), maxIndex+2);
	flag[maxIndex] =1;
}
if (POLICY == 4) { // Round Robin
	int maxIndex = 2;
	maxIndex = cloudlet.getVmId()%3;
	if (maxIndex !=0) 
		cloudlet.setCloudletLength ((int)(cloudlet.getCloudletLength() * slowdownTable[appId][peindex][maxIndex-1] ));
	getVmsToDatacentersMap().put(cloudlet.getVmId(), maxIndex+2);
	flag[maxIndex] =1;
}
if (POLICY == 5) { // static pe based
	int maxIndex = 0;
  if (peindex <=5)
	maxIndex = 1;
  if (peindex <=3)
	maxIndex = 2;

  if (maxIndex !=0) 
		cloudlet.setCloudletLength ((int)(cloudlet.getCloudletLength() * slowdownTable[appId][peindex][maxIndex-1] ));
	getVmsToDatacentersMap().put(cloudlet.getVmId(), maxIndex+2);
	flag[maxIndex] =1;
}

if (POLICY == 6) { // App based
	int maxIndex = 0;
  if (appId ==0 || appId==3 || appId ==5) // EP, Jacobi, Nq
	maxIndex = 2;
  if (appId == 1 || appId==6 ) // LU, Changa
	maxIndex = 1;

  if (maxIndex !=0) 
		cloudlet.setCloudletLength ((int)(cloudlet.getCloudletLength() * slowdownTable[appId][peindex][maxIndex-1] ));
	getVmsToDatacentersMap().put(cloudlet.getVmId(), maxIndex+2);
	flag[maxIndex] =1;
}
if (POLICY == 7) { // Demand Driven only
	int count =0;
	double maxEffFreePE =-1; 
	int maxIndex = 0;

	for (Integer freepes: freepesList) {
		if(count ==0) maxEffFreePE = freepes; 
else 	if(freepes > maxEffFreePE) {
                maxEffFreePE = freepes ; 
                 maxIndex = count;
                   } 
if(count !=0)  	Log.printLine(getName() + " maxEffFreePE:  " + maxEffFreePE  + "count " +count + "slowdown was " + slowdownTable[appId][peindex][count-1]  );
		count ++;
		}

  if (maxIndex !=0) 
		cloudlet.setCloudletLength ((int)(cloudlet.getCloudletLength() * slowdownTable[appId][peindex][maxIndex-1] ));
	getVmsToDatacentersMap().put(cloudlet.getVmId(), maxIndex+2);
	flag[maxIndex] =1;
}


if (POLICY == 8) { // Best first  only
	int count =0;
	double maxEffFreePE =-1; 
	int maxIndex = 0;

	for (Integer freepes: freepesList) {
		if(mypes<= freepes)  { maxIndex = count;break;}

		if(count ==0) maxEffFreePE = freepes; 
else 	if(freepes > maxEffFreePE) {
                maxEffFreePE = freepes ; 
                 maxIndex = count;
                   } 
		count ++;
		}

  if (maxIndex !=0) 
		cloudlet.setCloudletLength ((int)(cloudlet.getCloudletLength() * slowdownTable[appId][peindex][maxIndex-1] ));
	getVmsToDatacentersMap().put(cloudlet.getVmId(), maxIndex+2);
	flag[maxIndex] =1;
}




			}
		}
	}

for (Integer datacenterId : getDatacenterIdsList()) {
if(flag[datacenterId-2] ==1)
   createVmsInDatacenter(datacenterId);
}
// change cloudlet
// only if list not empty
 if(getCloudletList().size() !=0){
   float delay = 10; //contains the delay to the next periodic event
   boolean generatePeriodicEvent = true; //true if new internal events have to be generated
   if (generatePeriodicEvent) send(getId(),delay,CloudSimTags.PERIODIC_EVENT,getId());
}
 }


	/**
	 * Overrides this method when making a new and different type of Broker. This method is called
	 * by {@link #body()} for incoming unknown tags.
	 * 
	 * @param ev a SimEvent object
	 * @pre ev != null
	 * @post $none
	 */
	protected void processOtherEvent(SimEvent ev) {
		if (ev == null) {
			Log.printLine(getName() + ".processOtherEvent(): " + "Error - an event is null.");
			return;
		}

else {
     int tag = ev.getTag();
     switch(tag){
       case CloudSimTags.PERIODIC_EVENT: processPeriodicEvent(ev); break;
       default: Log.printLine("Warning: "+CloudSim.clock()+":"+this.getName()+": Unknown event ignored. Tag:" +tag);
     }
   }
 }


	/**
	 * Create the virtual machines in a datacenter.
	 * 
	 * @param datacenterId Id of the chosen PowerDatacenter
	 * @pre $none
	 * @post $none
	 */
	protected void createVmsInDatacenter(int datacenterId) {
		// send as much vms as possible for this datacenter before trying the next one
		int requestedVms = 0;
		String datacenterName = CloudSim.getEntityName(datacenterId);
	//	Log.printLine(CloudSim.clock() + ": " + getName() + ": Trying to Create VM " + " in # " + datacenterId+" name " + datacenterName);
		for (Vm vm : getVmList()) {
			if (getVmsToDatacentersMap().containsKey(vm.getId()))
			if (getVmsToDatacentersMap().get(vm.getId()) == datacenterId) {
			//	Log.printLine(CloudSim.clock() + ": " + getName() + ": Trying to Create VM #" + vm.getId()
			//			+ " in " + datacenterName);
				sendNow(datacenterId, CloudSimTags.VM_CREATE_ACK, vm);
				requestedVms++;
			}
		}

		getDatacenterRequestedIdsList().add(datacenterId);

		setVmsRequested(requestedVms);
		setVmsAcks(0);
	}

	/**
	 * Submit cloudlets to the created VMs.
	 * 
	 * @pre $none
	 * @post $none
	 */
	protected void submitCloudlets() {
		int vmIndex = 0;
		for (Cloudlet cloudlet : getCloudletList()) {
			Vm vm;
			// if user didn't bind this cloudlet and it has not been executed yet
			if (cloudlet.getVmId() == -1) {
				vm = getVmsCreatedList().get(vmIndex);
			} else { // submit to the specific vm
				vm = VmList.getById(getVmsCreatedList(), cloudlet.getVmId());
				if (vm == null) { // vm was not created
//					Log.printLine(CloudSim.clock() + ": " + getName() + ": Postponing execution of cloudlet "
//							+ cloudlet.getCloudletId() + ": bount VM not available");
					continue;
				}
			}

			Log.printLine(CloudSim.clock() + ": " + getName() + ": Sending cloudlet "
					+ cloudlet.getCloudletId() + " to VM #" + vm.getId());
			cloudlet.setVmId(vm.getId());
			sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
			cloudletsSubmitted++;
			vmIndex = (vmIndex + 1) % getVmsCreatedList().size();
			getCloudletSubmittedList().add(cloudlet);
		}

		// remove submitted cloudlets from waiting list
		for (Cloudlet cloudlet : getCloudletSubmittedList()) {
			getCloudletList().remove(cloudlet);
		}
	}

	/**
	 * Destroy the virtual machines running in datacenters.
	 * 
	 * @pre $none
	 * @post $none
	 */
	protected void clearDatacenters() {
		for (Vm vm : getVmsCreatedList()) {
			Log.printLine(CloudSim.clock() + ": " + getName() + ": Destroying VM #" + vm.getId());
			sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.VM_DESTROY, vm);
		}

		getVmsCreatedList().clear();
	}

	/**
	 * Send an internal event communicating the end of the simulation.
	 * 
	 * @pre $none
	 * @post $none
	 */
	protected void finishExecution() {
		sendNow(getId(), CloudSimTags.END_OF_SIMULATION);
	}

	/*
	 * (non-Javadoc)
	 * @see cloudsim.core.SimEntity#shutdownEntity()
	 */
	@Override
	public void shutdownEntity() {
		Log.printLine(getName() + " is shutting down...");
	}

	/*
	 * (non-Javadoc)
	 * @see cloudsim.core.SimEntity#startEntity()
	 */
	@Override
	public void startEntity() {
		Log.printLine(getName() + " is starting...");
		schedule(getId(), 0, CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST);
	}

	/**
	 * Gets the vm list.
	 * 
	 * @param <T> the generic type
	 * @return the vm list
	 */
	@SuppressWarnings("unchecked")
	public <T extends Vm> List<T> getVmList() {
		return (List<T>) vmList;
	}

	/**
	 * Sets the vm list.
	 * 
	 * @param <T> the generic type
	 * @param vmList the new vm list
	 */
	protected <T extends Vm> void setVmList(List<T> vmList) {
		this.vmList = vmList;
	}

	/**
	 * Gets the cloudlet list.
	 * 
	 * @param <T> the generic type
	 * @return the cloudlet list
	 */
	@SuppressWarnings("unchecked")
	public <T extends Cloudlet> List<T> getCloudletList() {
		return (List<T>) cloudletList;
	}

	/**
	 * Sets the cloudlet list.
	 * 
	 * @param <T> the generic type
	 * @param cloudletList the new cloudlet list
	 */
	protected <T extends Cloudlet> void setCloudletList(List<T> cloudletList) {
		this.cloudletList = cloudletList;
	}

	/**
	 * Gets the cloudlet submitted list.
	 * 
	 * @param <T> the generic type
	 * @return the cloudlet submitted list
	 */
	@SuppressWarnings("unchecked")
	public <T extends Cloudlet> List<T> getCloudletSubmittedList() {
		return (List<T>) cloudletSubmittedList;
	}

	/**
	 * Sets the cloudlet submitted list.
	 * 
	 * @param <T> the generic type
	 * @param cloudletSubmittedList the new cloudlet submitted list
	 */
	protected <T extends Cloudlet> void setCloudletSubmittedList(List<T> cloudletSubmittedList) {
		this.cloudletSubmittedList = cloudletSubmittedList;
	}

	/**
	 * Gets the cloudlet received list.
	 * 
	 * @param <T> the generic type
	 * @return the cloudlet received list
	 */
	@SuppressWarnings("unchecked")
	public <T extends Cloudlet> List<T> getCloudletReceivedList() {
		return (List<T>) cloudletReceivedList;
	}

	/**
	 * Sets the cloudlet received list.
	 * 
	 * @param <T> the generic type
	 * @param cloudletReceivedList the new cloudlet received list
	 */
	protected <T extends Cloudlet> void setCloudletReceivedList(List<T> cloudletReceivedList) {
		this.cloudletReceivedList = cloudletReceivedList;
	}

	/**
	 * Gets the vm list.
	 * 
	 * @param <T> the generic type
	 * @return the vm list
	 */
	@SuppressWarnings("unchecked")
	public <T extends Vm> List<T> getVmsCreatedList() {
		return (List<T>) vmsCreatedList;
	}

	/**
	 * Sets the vm list.
	 * 
	 * @param <T> the generic type
	 * @param vmsCreatedList the vms created list
	 */
	protected <T extends Vm> void setVmsCreatedList(List<T> vmsCreatedList) {
		this.vmsCreatedList = vmsCreatedList;
	}

	/**
	 * Gets the vms requested.
	 * 
	 * @return the vms requested
	 */
	protected int getVmsRequested() {
		return vmsRequested;
	}

	/**
	 * Sets the vms requested.
	 * 
	 * @param vmsRequested the new vms requested
	 */
	protected void setVmsRequested(int vmsRequested) {
		this.vmsRequested = vmsRequested;
	}

	/**
	 * Gets the vms acks.
	 * 
	 * @return the vms acks
	 */
	protected int getVmsAcks() {
		return vmsAcks;
	}

	/**
	 * Sets the vms acks.
	 * 
	 * @param vmsAcks the new vms acks
	 */
	protected void setVmsAcks(int vmsAcks) {
		this.vmsAcks = vmsAcks;
	}

	/**
	 * Increment vms acks.
	 */
	protected void incrementVmsAcks() {
		vmsAcks++;
	}

	/**
	 * Gets the vms destroyed.
	 * 
	 * @return the vms destroyed
	 */
	protected int getVmsDestroyed() {
		return vmsDestroyed;
	}

	/**
	 * Sets the vms destroyed.
	 * 
	 * @param vmsDestroyed the new vms destroyed
	 */
	protected void setVmsDestroyed(int vmsDestroyed) {
		this.vmsDestroyed = vmsDestroyed;
	}

	/**
	 * Gets the datacenter ids list.
	 * 
	 * @return the datacenter ids list
	 */
	protected List<Integer> getDatacenterIdsList() {
		return datacenterIdsList;
	}

	/**
	 * Sets the datacenter ids list.
	 * 
	 * @param datacenterIdsList the new datacenter ids list
	 */
	protected void setDatacenterIdsList(List<Integer> datacenterIdsList) {
		this.datacenterIdsList = datacenterIdsList;
	}

	/**
	 * Gets the vms to datacenters map.
	 * 
	 * @return the vms to datacenters map
	 */
	protected Map<Integer, Integer> getVmsToDatacentersMap() {
		return vmsToDatacentersMap;
	}

	/**
	 * Sets the vms to datacenters map.
	 * 
	 * @param vmsToDatacentersMap the vms to datacenters map
	 */
	public void setVmsToDatacentersMap(Map<Integer, Integer> vmsToDatacentersMap) {
		this.vmsToDatacentersMap = vmsToDatacentersMap;
	}

	/**
	 * Gets the datacenter characteristics list.
	 * 
	 * @return the datacenter characteristics list
	 */
	protected Map<Integer, DatacenterCharacteristics> getDatacenterCharacteristicsList() {
		return datacenterCharacteristicsList;
	}

	/**
	 * Sets the datacenter characteristics list.
	 * 
	 * @param datacenterCharacteristicsList the datacenter characteristics list
	 */
	protected void setDatacenterCharacteristicsList(
			Map<Integer, DatacenterCharacteristics> datacenterCharacteristicsList) {
		this.datacenterCharacteristicsList = datacenterCharacteristicsList;
	}

	/**
	 * Gets the datacenter requested ids list.
	 * 
	 * @return the datacenter requested ids list
	 */
	protected List<Integer> getDatacenterRequestedIdsList() {
		return datacenterRequestedIdsList;
	}

	/**
	 * Sets the datacenter requested ids list.
	 * 
	 * @param datacenterRequestedIdsList the new datacenter requested ids list
	 */
	protected void setDatacenterRequestedIdsList(List<Integer> datacenterRequestedIdsList) {
		this.datacenterRequestedIdsList = datacenterRequestedIdsList;
	}

}
