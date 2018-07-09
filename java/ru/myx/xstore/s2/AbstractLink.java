/**
 * 
 */
package ru.myx.xstore.s2;

import ru.myx.ae1.storage.BaseVersion;

/**
 * @author myx
 * 
 */
public abstract class AbstractLink implements BaseLink {
	@Override
	public String[] getAliases() {
		return null;
	}
	
	@Override
	public BaseLink getVersion(final String versionId) {
		return this;
	}
	
	@Override
	public String getVersionId() {
		return null;
	}
	
	@Override
	public boolean getVersioning() {
		return false;
	}
	
	@Override
	public BaseVersion[] getVersions() {
		return null;
	}
	
	@Override
	public void invalidateThis() {
		// do nothing
	}
	
	@Override
	public void invalidateTree() {
		// do nothing
	}
}
