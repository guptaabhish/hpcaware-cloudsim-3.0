/*
 * Title:        CloudSim Toolkit
 * Description:  CloudSim (Cloud Simulation) Toolkit for Modeling and Simulation of Clouds
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2009-2012, The University of Melbourne, Australia
 */

package org.cloudbus.cloudsim;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.cloudbus.cloudsim.core.CloudSim;

/**
 * VmAllocationPolicySimple is an VmAllocationPolicy that chooses, as the host
 * for a VM, the host with less PEs in use.
 * 
 * @author Rodrigo N. Calheiros
 * @author Anton Beloglazov
 * @since CloudSim Toolkit 1.0
 */
public class VmAllocationPolicySimple extends VmAllocationPolicy {

	/** The vm table. */
	private Map<String,List<Host>> vmTable;

	/** The used pes. */
	private Map<String, Integer> usedPes;

	/** The free pes. */
	private List<Integer> freePes;
	int freeCount;
	private Map<Integer, Integer> hostPEMap;
	private Map<Integer, Integer> hostRamMap;
	private Map<Integer, Long> hostCacheScoreMap;
	public Map<Integer, Long> getHostCacheScoreMap() {
		return hostCacheScoreMap;
	}
	private int fcfsPause;
	public void setHostCacheScoreMap(Map<Integer, Long> hostCacheScoreMap) {
		this.hostCacheScoreMap = hostCacheScoreMap;
	}

	/**
	 * Creates the new VmAllocationPolicySimple object.
	 * 
	 * @param list
	 *            the list
	 * @pre $none
	 * @post $none
	 */
	public VmAllocationPolicySimple(List<? extends Host> list) {
		super(list);
	//	Log.printLine("hostPE host &&&:"+getHostPEMap());
		setFreePes(new ArrayList<Integer>()); 
		freeCount=0;

		setHostPEMap(new TreeMap<Integer, Integer>());
		setHostRamMap(new TreeMap<Integer, Integer>());
		setHostCacheScoreMap(new TreeMap<Integer,Long>());

		for (Host host : getHostList()) {
			getFreePes().add(host.getNumberOfPes());
			freeCount++;
			getHostRamMap().put(host.getId(), host.getRam());
			getHostCacheScoreMap().put(host.getId(),host.getAvailableCacheScore());
			if (host.getNumberOfPes() > 0)
				getHostPEMap().put(host.getId(), host.getNumberOfPes());

		}

		setVmTable(new HashMap<String, List<Host>>());
		setUsedPes(new HashMap<String, Integer>());
		fcfsPause = 0;
	}

	public Map<Integer, Integer> getHostRamMap() {
		return hostRamMap;
	}

	public void setHostRamMap(Map<Integer, Integer> hostRamMap) {
		this.hostRamMap = hostRamMap;
	}

	public Map<Integer, Integer> getHostPEMap() {
		return hostPEMap;
	}

	public void setHostPEMap(Map<Integer, Integer> hostPEMap) {
		this.hostPEMap = hostPEMap;
	}

	// sort any map in reverse order.
	private static Map sortByComparator(Map unsortMap) {

		List list = new LinkedList(unsortMap.entrySet());

		// sort list based on comparator
		Collections.sort(list, new Comparator() {
			public int compare(Object o1, Object o2) {
				return ((Comparable) ((Map.Entry) (o2)).getValue())
						.compareTo(((Map.Entry) (o1)).getValue());
			}
		});

		// put sorted list into map again
		Map sortedMap = new LinkedHashMap();
		for (Iterator it = list.iterator(); it.hasNext();) {
			Map.Entry entry = (Map.Entry) it.next();
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}

	
	public boolean allocateHostForVm(Vm vm) {
		if(fcfsPause ==1)
			{	//Log.printLine("FCFS wait"); 
                     return false;}

		int requiredPes = vm.getNumberOfPes();
		int requiredRam = vm.getRam();
		long requiredCacheScore=vm.getCacheScore();
		boolean result = false;

		//	Log.printLine("Inside allocate ");
		Map<Integer, Integer> weightedValue = new TreeMap<Integer, Integer>();
		// this is the map of host ID and available PE in it.
		Map<Integer, Integer> hostPESortedMap = getHostPEMap();
		// this is the map of host ID and available ram in it.
		Map<Integer, Integer> hostRamMap = getHostRamMap();
		Map<Integer, Long> hostCacheScoreMap = getHostCacheScoreMap();
		//  Log.printLine("VM ID:"+vm.getUid());
		//  Log.printLine("requiredPes:"+requiredPes);
		 // Log.printLine("requiredRam:"+requiredRam);
		  
		List<Host> hostsPrev = getVmTable().get(vm.getUid());
		if (hostsPrev!=null) 
			{	Log.printLine("Already Provisioned"); return false;}
		List<Host> hosts=new ArrayList<Host>();

		if (hostPESortedMap.size() > 0) {
			int idx = -1;
			for (int j = 1; j <= requiredPes; j++) {
				result = false;
				// the for loop calculate the weighted value of each host based
				// on its available ram and required ram and available PE and
				// required PE
				
				if (idx > -1) {
					weightedValue.put(idx,					((hostPESortedMap.get(idx) * requiredPes)/ (1 * 1) +  (hostRamMap.get(idx) * requiredRam) / (512 * 512)));
/* LEAST FREE PE*/ 	//	weightedValue.put(idx,	((hostPESortedMap.get(idx) *-1) ));
/*MOST FREE PE*/	 //		((hostPESortedMap.get(idx)) ));
/* LEAST MEM*/ 	 	//	weightedValue.put(idx,		 (hostRamMap.get(idx)  *-1));
/* MOST MEM*/ //				( (hostRamMap.get(idx)  )));
				} else {
					for (Map.Entry<Integer, Integer> entry : hostPESortedMap.entrySet()) {

		weightedValue.put(entry.getKey(),((entry.getValue() * requiredPes) / (1 * 1) +  (hostRamMap.get(entry.getKey()) * requiredRam)	/ (512 *512)));
	/* LEAST FREE PE*/  //weightedValue.put(entry.getKey(),((entry.getValue()  * -1)));
	/* MOST FREE PE*/ // weightedValue.put(entry.getKey(),((entry.getValue())));
 /* LEAST MEM*/	//weightedValue.put(entry.getKey(), hostRamMap.get(entry.getKey()*-1) );
 /*MOST MEM*///		weightedValue.put(entry.getKey(), hostRamMap.get(entry.getKey()) );

					}
				}
				// this is the reverse sorted map of hostID and its weighted value
				Map<Integer, Integer> weightedValueSortedMap = sortByComparator(weightedValue); 
				
			//	Log.printLine("freePEs:"+hostPESortedMap);
			//	 Log.printLine("freeRams:"+hostRamMap);
			//	 Log.printLine("weightedValueSortedMap:"
			//	 +weightedValueSortedMap);
				 idx =-1;
				for (Map.Entry<Integer, Integer> entry : weightedValueSortedMap
						.entrySet()) {

					if (hostPESortedMap.get(entry.getKey()) >= 1
							&& hostRamMap.get(entry.getKey()) >= requiredRam && hostCacheScoreMap.get(entry.getKey())>requiredCacheScore) {
						idx = entry.getKey();
						break;
					}else
						continue;

				}
				/* Log.printLine("host id:"+idx); */
				if (idx >= 0) {
					Host host = getHostList().get(idx);

					vm.setNumberOfPes(1);

					/*
					 * Log.printLine("vm:"+vm.getCurrentRequestedMips());
					 * Log.printLine("host:"+hostPESortedMap.get(host.getId()));
					 */result = host.vmCreate(vm);
					Log.printLine("result:" + result);
					if (result) {
						hosts.add(host);
					Log.printLine("hostlist"  + hosts+"  " +host);
						getVmTable().put(vm.getUid(), hosts);
					Log.printLine("vmtable"  + getVmTable().get(vm.getUid()));
						
						getUsedPes().put(vm.getUid(), 1);
						if (getHostRamMap().get(idx) != null
								&& getHostRamMap().get(idx) - requiredRam >= 0) {
							getHostRamMap().put(idx,
									getHostRamMap().get(idx) - requiredRam);
						}
						if (getHostPEMap().get(idx) != null
								&& getHostPEMap().get(idx) - 1 >=0) {

							getHostPEMap()
									.put(idx, getHostPEMap().get(idx) - 1);
						}
						if(getHostCacheScoreMap().get(idx)!=null 
								&& getHostCacheScoreMap().get(idx)-requiredCacheScore>=0){
							getHostCacheScoreMap().put(idx, getHostCacheScoreMap().get(idx)-requiredCacheScore);
						}
					}
    else {
                if(j >1 ) { deallocateHostForVm(vm);
					//Log.printLine("result:"  + j);
			}
					vm.setNumberOfPes(requiredPes);
                break;
                }


				}
   else {
                if(j >1 ) { deallocateHostForVm(vm);
                                        //Log.printLine("result:"  + j);
				}
                                        vm.setNumberOfPes(requiredPes);
                break;
                }

			}
			
		}
//
               Log.printLine("result1:" +result );
		if(result) freeCount = freeCount - requiredPes;
		if(!result) fcfsPause = 1; // pause scheduling
		return result;
	}

	/**
	 * Allocates a host for a given VM.
	 * 
	 * @param vm
	 *            VM specification
	 * @return $true if the host could be allocated; $false otherwise
	 * @pre $none
	 * @post $none
	 */
	/*
	 * @Override public boolean allocateHostForVm(Vm vm) {
	 * Log.printLine("allocateHostForVm method..."); int requiredPes =
	 * vm.getNumberOfPes(); boolean result = false; int tries = 0; List<Integer>
	 * freePesTmp = new ArrayList<Integer>();
	 * 
	 * for (Integer freePes : getFreePes()) { freePesTmp.add(freePes);
	 * Log.printLine("freePEs old method:"+freePes); }
	 * 
	 * if (!getVmTable().containsKey(vm.getUid())) { // if this vm was not
	 * created do {// we still trying until we find a host or until we try all
	 * of them int moreFree = Integer.MIN_VALUE; int idx = -1;
	 * 
	 * // we want the host with less pes in use for (int i = 0; i <
	 * freePesTmp.size(); i++) { if (freePesTmp.get(i) > moreFree) { moreFree =
	 * freePesTmp.get(i); idx = i; } }
	 * 
	 * Host host = getHostList().get(idx);
	 * 
	 * result = host.vmCreate(vm);
	 * 
	 * if (result) { // if vm were succesfully created in the host
	 * getVmTable().put(vm.getUid(), host); getUsedPes().put(vm.getUid(),
	 * requiredPes); getFreePes().set(idx, getFreePes().get(idx) - requiredPes);
	 * result = true; break; } else { freePesTmp.set(idx, Integer.MIN_VALUE); }
	 * tries++; } while (!result && tries < getFreePes().size());
	 * 
	 * } if(!result){ result=allocateHostsForVm(vm) ; } return result; }
	 */

	/**
	 * Releases the host used by a VM.
	 * 
	 * @param vm
	 *            the vm
	 * @pre $none
	 * @post none
	 */
	@Override
	public void deallocateHostForVm(Vm vm) {
		
		fcfsPause = 0; // no pause now
		List<Host> hosts = getVmTable().remove(vm.getUid());
		if (hosts==null)	{	//Log.printLine("Already deProvisioned"); 
	return ;}
		for(Host host:hosts){
                                        //Log.printLine("crash"  + vm.getUid());
			int idx = getHostList().indexOf(host);
			
		//	int pes = getUsedPes().remove(vm.getUid());
			if (host != null) {
				host.vmDestroy(vm);
				getHostPEMap()
				.put(idx, getHostPEMap().get(idx) + 1);
				getHostCacheScoreMap().put(idx, getHostCacheScoreMap().get(idx)+vm.getCacheScore());
				getHostRamMap().put(idx,
						getHostRamMap().get(idx) + vm.getRam());
				getFreePes().set(idx, getFreePes().get(idx) + 1);
				freeCount++;
			}
		}
		
	}

	/**
	 * Gets the host that is executing the given VM belonging to the given user.
	 * 
	 * @param vm
	 *            the vm
	 * @return the Host with the given vmID and userID; $null if not found
	 * @pre $none
	 * @post $none
	 */
	@Override
	public Host getHost(Vm vm) {
		return getVmTable().get(vm.getUid()).get(0);
	}
	
	public List<Host> getHosts(Vm vm) {
		return getVmTable().get(vm.getUid());
	}

	/**
	 * Gets the host that is executing the given VM belonging to the given user.
	 * 
	 * @param vmId
	 *            the vm id
	 * @param userId
	 *            the user id
	 * @return the Host with the given vmID and userID; $null if not found
	 * @pre $none
	 * @post $none
	 */
	@Override
	public Host getHost(int vmId, int userId) {
		return getVmTable().get(Vm.getUid(userId, vmId)).get(0);
	}

	public List<Host> getHosts(int vmId, int userId) {
//		Log.printLine("Hello" +  getVmTable().get(Vm.getUid(userId, vmId)));
		return getVmTable().get(Vm.getUid(userId, vmId));
	}
	/**
	 * Gets the vm table.
	 * 
	 * @return the vm table
	 */
	public Map<String,List<Host>> getVmTable() {
		return vmTable;
	}

	/**
	 * Sets the vm table.
	 * 
	 * @param vmTable
	 *            the vm table
	 */
	protected void setVmTable(Map<String,List<Host>> vmTable) {
		this.vmTable = vmTable;
	}

	/**
	 * Gets the used pes.
	 * 
	 * @return the used pes
	 */
	protected Map<String, Integer> getUsedPes() {
		return usedPes;
	}

	/**
	 * Sets the used pes.
	 * 
	 * @param usedPes
	 *            the used pes
	 */
	protected void setUsedPes(Map<String, Integer> usedPes) {
		this.usedPes = usedPes;
	}

	/**
	 * Gets the free pes.
	 * 
	 * @return the free pes
	 */
	public List<Integer> getFreePes() {
		return freePes;
	}

	/**
	 * Sets the free pes.
	 * 
	 * @param freePes
	 *            the new free pes
	 */
	protected void setFreePes(List<Integer> freePes) {
		this.freePes = freePes;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see cloudsim.VmAllocationPolicy#optimizeAllocation(double,
	 * cloudsim.VmList, double)
	 */
	@Override
	public List<Map<String, Object>> optimizeAllocation(
			List<? extends Vm> vmList) {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.cloudbus.cloudsim.VmAllocationPolicy#allocateHostForVm(org.cloudbus
	 * .cloudsim.Vm, org.cloudbus.cloudsim.Host)
	 */
	@Override
	public boolean allocateHostForVm(Vm vm, Host host) {
		List<Host> hosts=new ArrayList<Host>();
		hosts.add(host);
		if (host.vmCreate(vm)) { // if vm has been succesfully created in the
									// host
			getVmTable().put(vm.getUid(), hosts);

			//int requiredPes = vm.getNumberOfPes();
			int idx = getHostList().indexOf(host);
			getUsedPes().put(vm.getUid(), 1);
			getFreePes().set(idx, getFreePes().get(idx) - 1);
			freeCount--;
			Log.formatLine("%.2f: VM #" + vm.getId()
					+ " has been allocated to the host #" + host.getId(),
					CloudSim.clock());
			return true;
		}

		return false;
	}
}
