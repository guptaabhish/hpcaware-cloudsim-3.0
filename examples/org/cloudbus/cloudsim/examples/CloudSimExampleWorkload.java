package org.cloudbus.cloudsim.examples;

/*
 * Title:        CloudSim Toolkit
 * Description:  CloudSim (Cloud Simulation) Toolkit for Modeling and Simulation
 *               of Clouds
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2009, The University of Melbourne, Australia
 */

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.*;
    import java.util.Random;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.cloudbus.cloudsim.util.WorkloadFileReader;

/**
 * A simple example showing how to create a datacenter with one host and run one
 * cloudlet on it.
 */
public class CloudSimExampleWorkload {

    /** The cloudlet list. */
    private static List<Cloudlet> cloudletList;

    /** The vmlist. */
    private static List<Vm> vmlist;

    private static List<Cloudlet> cloudletList1;

    /** The vmlist. */
    private static List<Vm> vmlist1;

    /**
     * Creates main() to run this example.
     * 
     * @param args
     *        the args
     */
    public static void main(String[] args) {

	Log.printLine("Starting CloudSimExampleWorkload new...");

	try {

	int firstArg; int mypolicy = 0;
if (args.length > 0) {
       mypolicy = Integer.parseInt(args[0]); }
	    // First step: Initialize the CloudSim package. It should be called
	    // before creating any entities.
	    int num_user = 1; 
	    int stfg=2;// number of cloud users
	    Calendar calendar = Calendar.getInstance();
	    boolean trace_flag = false; // mean trace events

	    // Initialize the CloudSim library
	    CloudSim.init(num_user, calendar, trace_flag);

	    // Second step: Create Datacenters
	    // Datacenters are the resource providers in CloudSim. We need at
	    // list one of them to run a CloudSim simulation
	    Datacenter datacenter0 = createDatacenter("Supercomputer", 32);
//	  
    Datacenter datacenter1 = createDatacenter("Cluster", 48); 
    Datacenter datacenter2 = createDatacenter("Cloud", 48); 

	    // Third step: Create Broker
	    DatacenterBroker broker = createBroker();
	    int brokerId = broker.getId();
	  //  DatacenterBroker broker1 = createBroker1();
	  // int brokerId1 = broker1.getId();
		broker.POLICY = mypolicy;
	    // Fourth step: Create one virtual machine
	    vmlist = new ArrayList<Vm>();

	    // VM description
	    int vmid = 0;
	    int mips = 1000;
	    long size = 10000; // image size (MB)
	    int ram = 512; // vm memory (MB)
	    long bw = 1;
	    int pesNumber = 1; // number of cpus
	    String vmm = "Xen"; // VMM name

	    long cacheScore =0;
     Random generator = new Random();
	    // Fifth step: Read Cloudlets from workload file in the swf format
	    WorkloadFileReader workloadFileReader = new WorkloadFileReader("METACENTRUM-2009-2.swf", 1);
	    cloudletList = workloadFileReader.generateWorkload().subList(0, 1000);
	    for (Cloudlet cloudlet : cloudletList) {
	    cacheScore =    generator.nextInt(5 ) ;
  	//	ram= (1024);	
  	//	ram= (int)(cloudlet.getCloudletFileSize()/cloudlet.getNumberOfPes() ) + 200000;
//	 if (cloudlet.getCloudletFileSize()/cloudlet.getNumberOfPes() > 1024000 )
  		ram= (1024);	
		 Log.printLine("Cloudlet Size : "+ram);
		 Log.printLine("Score : "+cacheScore);
		 Log.printLine("Number of PEs : "+cloudlet.getNumberOfPes());
			 cloudlet.setCloudletLength( (long)(2*cloudlet.getCloudletLength()));
		// create VM
	    Vm vm = new Vm(vmid, brokerId, mips, cloudlet.getNumberOfPes(), (int)(ram), bw, size, vmm, new CloudletSchedulerTimeShared());
              vm.setCacheScore(cacheScore);
	    cloudlet.setUserId(brokerId);
		cloudlet.setVmId(vmid);
		cloudlet.setNumberOfPes(1);
	    vmid++;
	    Log.printLine("vm.getUid():"+vm.getUid());
	    // add the VM to the vmList
	    vmlist.add(vm);
	   
	    // submit vm list to the broker
		
		
	    }
	
/***************************************/

//	    Datacenter datacenter1 = createDatacenter("Datacenter_1");

	    // Third step: Create Broker

	    // Fourth step: Create one virtual machine
	    // VM description
	  
	     // Fifth step: Read Cloudlets from workload file in the swf format
	//    WorkloadFileReader workloadFileReader = new WorkloadFileReader("METACENTRUM-2009-2.swf", 1);


    broker.submitVmList(vmlist);

	    // submit cloudlet list to the broker

	 //   broker1.submitVmList(vmlist1);

	    broker.submitCloudletList(cloudletList);
	    // submit cloudlet list to the broker
	  //  broker1.submitCloudletList(cloudletList1);
      HashMap m1 = new HashMap <Integer, Integer>();
for (int i = 0; i < vmid; i ++)
if(i < 50) 	m1.put(i, 0+2);	
else 
 	m1.put(i, 1+2);	
//	broker.setVmsToDatacentersMap(m1);






/*********************************************/
	    //printCloudletList1(cloudletList);

	    // Sixth step: Starts the simulation
	    CloudSim.startSimulation();
	    CloudSim.stopSimulation();

	    // Final step: Print results when simulation is over
	    List<Cloudlet> newList = broker.getCloudletReceivedList();
	    printCloudletList(newList);

	    // Print the debt of each user to each datacenter
	    datacenter0.printDebts();

	    Log.printLine("CloudSimExample1 finished!");
	} catch (Exception e) {
	    e.printStackTrace();
	    Log.printLine("Unwanted errors happen");
	}
    }

    /**
     * Creates the datacenter.
     * 
     * @param name
     *        the name
     * 
     * @return the datacenter
     */
    private static Datacenter createDatacenter(String name, int nodes) {

	// Here are the steps needed to create a PowerDatacenter:
	// 1. We need to create a list to store
	// our machine
	List<Host> hostList = new ArrayList<Host>();

	// 2. A Machine contains one or more PEs or CPUs/Cores.
	
	List<Pe> peList = new ArrayList<Pe>();

	int mips = 1000;

	// 3. Create PEs and add these into a list.
	peList.add(new Pe(0, new PeProvisionerSimple(mips))); // need to store
//	peList.add(new Pe(1, new PeProvisionerSimple(mips))); // need to store
//	peList.add(new Pe(2, new PeProvisionerSimple(mips))); // need to store
//	peList.add(new Pe(3, new PeProvisionerSimple(mips)));
/*	peList.add(new Pe(4, new PeProvisionerSimple(mips)));
	peList.add(new Pe(5, new PeProvisionerSimple(mips)));
	peList.add(new Pe(6, new PeProvisionerSimple(mips)));
	peList.add(new Pe(7, new PeProvisionerSimple(mips)));
	peList.add(new Pe(8, new PeProvisionerSimple(mips)));
	peList.add(new Pe(9, new PeProvisionerSimple(mips)));
	peList.add(new Pe(10, new PeProvisionerSimple(mips)));
	peList.add(new Pe(11, new PeProvisionerSimple(mips)));
	peList.add(new Pe(12, new PeProvisionerSimple(mips)));
	peList.add(new Pe(13, new PeProvisionerSimple(mips)));
	peList.add(new Pe(14, new PeProvisionerSimple(mips)));
	peList.add(new Pe(15, new PeProvisionerSimple(mips)));
*/								      // Pe id and MIPS
							      // Rating

	// 4. Create Host with its id and list of PEs and add them to the list
	// of machines
	int hostId = 0;
	int ram =1024000; // host memory (MB)
	long storage = 1000000; // host storage
	int bw = 10000;
/*	for (int i=0;i<256;i++){
		hostList.add(new Host(hostId, new RamProvisionerSimple(ram), new BwProvisionerSimple(bw),
			    storage, peList, new VmSchedulerTimeShared(peList)));
			hostId++;
	}
*/			
    long cacheScore=40;

      // for (int i=0;i<256;i++){
       for (int i=0;i<nodes;i++){

              Host host=new  Host(hostId, new RamProvisionerSimple(ram), new BwProvisionerSimple(bw),

                         storage, peList, new VmSchedulerTimeShared(peList));

              host.setAvailableCacheScore(cacheScore);

              hostList.add(host);

                     hostId++;
}
	// 5. Create a DatacenterCharacteristics object that stores the
	// properties of a data center: architecture, OS, list of
	// Machines, allocation policy: time- or space-shared, time zone
	// and its price (G$/Pe time unit).
	String arch = "x86"; // system architecture
	String os = "Linux"; // operating system
	String vmm = "Xen";
	double time_zone = 10.0; // time zone this resource located
	double cost = 3.0; // the cost of using processing in this resource
	double costPerMem = 0.05; // the cost of using memory in this resource
	double costPerStorage = 0.001; // the cost of using storage in this
				       // resource
	double costPerBw = 0.0; // the cost of using bw in this resource
	LinkedList<Storage> storageList = new LinkedList<Storage>(); // we are
								     // not
								     // adding
								     // SAN
								     // devices
								     // by now

	DatacenterCharacteristics characteristics = new DatacenterCharacteristics(arch, os, vmm,
	    hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);

	// 6. Finally, we need to create a PowerDatacenter object.
	Datacenter datacenter = null;
	try {
	    datacenter = new Datacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
	} catch (Exception e) {
	    e.printStackTrace();
	}
	return datacenter;
    }

    // We strongly encourage users to develop their own broker policies, to
    // submit vms and cloudlets according
    // to the specific rules of the simulated scenario
    /**
     * Creates the broker.
     * 
     * @return the datacenter broker
     */
    private static DatacenterBroker createBroker() {
	DatacenterBroker broker = null;
	try {
	    broker = new DatacenterBroker("Broker");
	} catch (Exception e) {
	    e.printStackTrace();
	    return null;
	}
	return broker;
    }

  private static DatacenterBroker createBroker1() {
	DatacenterBroker broker = null;
	try {
	    broker = new DatacenterBroker("Broker1");
	} catch (Exception e) {
	    e.printStackTrace();
	    return null;
	}
	return broker;
    }

  private static void printCloudletList1(List<Cloudlet> list) {
	int size = list.size();
	Cloudlet cloudlet;
	String indent = "    ";
	Log.printLine();
	Log.printLine("========== OUTPUT ==========");
	Log.printLine("Cloudlet ID"
		+ indent
		+ "STATUS"
		+ indent
		+ "Data center ID"
		+ indent
		+ "VM ID"
		+ indent
		+ "Time"
		+ indent
		+ "Start Time"
		+ indent
		+ "Finish Time");

	DecimalFormat dft = new DecimalFormat("###.##");
	for (int i = 0; i < size; i++) {
	    cloudlet = list.get(i);
	    Log.print(indent + cloudlet.getCloudletId() + indent + indent);

	 {
		Log.print("SUCCESS");

		Log.printLine(indent
			+ indent
			+ cloudlet.getNumberOfPes()
			+ indent
			+ cloudlet.getCloudletFileSize()
			+ indent
			+ cloudlet.getCloudletLength());
	    }
	}
    }


    /**
     * Prints the Cloudlet objects.
     * 
     * @param list
     *        list of Cloudlets
     */
    private static void printCloudletList(List<Cloudlet> list) {
	int size = list.size();
	Cloudlet cloudlet;

	String indent = "    ";
	Log.printLine();
	Log.printLine("========== OUTPUT ==========");
	Log.printLine("Cloudlet ID"
		+ indent
		+ "STATUS"
		+ indent
		+ "Data center ID"
		+ indent
		+ "VM ID"
		+ indent
		+ "Time"
		+ indent
		+ "Start Time"
		+ indent
		+ "Finish Time");

	DecimalFormat dft = new DecimalFormat("###.##");
	for (int i = 0; i < size; i++) {
	    cloudlet = list.get(i);
	    Log.print(indent + cloudlet.getCloudletId() + indent + indent);

	    if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS) {
		Log.print("SUCCESS");

		Log.printLine(indent
			+ indent
			+ cloudlet.getResourceId()
			+ indent
			+ indent
			+ indent
			+ cloudlet.getVmId()
			+ indent
			+ indent
			+ dft.format(cloudlet.getActualCPUTime())
			+ indent
			+ indent
			+ dft.format(cloudlet.getExecStartTime())
			+ indent
			+ indent
			+ dft.format(cloudlet.getFinishTime()));
	    }
	}
    }

}
