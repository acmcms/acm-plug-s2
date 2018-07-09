/*
 * Created on 21.08.2004
 * 
 * Window - Preferences - Java - Code Style - Code Templates
 */
package ru.myx.xstore.s2.ram;

import ru.myx.ae1.storage.BaseRecycled;

/**
 * @author myx
 * 
 *         Window - Preferences - Java - Code Style - Code Templates
 */
final class RecycledCached implements BaseRecycled {
	private final ServerRamCache	server;
	
	private final BaseRecycled		recycled;
	
	RecycledCached(final ServerRamCache server, final BaseRecycled recycled) {
		this.server = server;
		this.recycled = recycled;
	}
	
	@Override
	public boolean canClean() {
		return this.recycled.canClean();
	}
	
	@Override
	public boolean canMove() {
		return this.recycled.canMove();
	}
	
	@Override
	public boolean canRestore() {
		return this.recycled.canRestore();
	}
	
	@Override
	public void doClean() {
		this.recycled.doClean();
	}
	
	@Override
	public void doMove(final String parentGuid) {
		this.recycled.doMove( parentGuid );
		this.server.clear();
	}
	
	@Override
	public void doRestore() {
		this.recycled.doRestore();
		this.server.clear();
	}
	
	@Override
	public long getDate() {
		return this.recycled.getDate();
	}
	
	@Override
	public String getFolder() {
		return this.recycled.getFolder();
	}
	
	@Override
	public String getGuid() {
		return this.recycled.getGuid();
	}
	
	@Override
	public String getOwner() {
		return this.recycled.getOwner();
	}
	
	@Override
	public String getTitle() {
		return this.recycled.getTitle();
	}
}
