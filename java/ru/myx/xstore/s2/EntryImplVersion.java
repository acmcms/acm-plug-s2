/*
 * Created on 10.11.2005
 */
package ru.myx.xstore.s2;

import ru.myx.ae1.storage.BaseChange;
import ru.myx.ae1.storage.StorageImpl;

final class EntryImplVersion extends EntryImpl {
	
	private final CurrentStorage system;
	
	private final EntryImpl entry;
	
	private final String versionId;
	
	EntryImplVersion(final StorageImpl storage, final CurrentStorage system, final EntryImpl entry, final BaseLink link, final String versionId) {
		super(storage, system, link);
		this.system = system;
		this.entry = entry;
		this.versionId = versionId;
	}
	
	@Override
	public BaseChange createChange() {
		
		return new ChangeForVersion(this.system, this, this.versionId, this.entry.getOriginalLink(), this.entry.getData());
	}
	
	@Override
	public String toString() {
		
		return "[object " + this.baseClass() + "(" + "id=" + this.getGuid() + ", versionId=" + this.versionId + ", title=" + this.getTitle() + ")]";
	}
}
