/*
 * Created on 10.11.2005
 */
package ru.myx.xstore.s2;

import ru.myx.ae1.storage.BaseChange;
import ru.myx.ae1.storage.StorageImpl;

final class EntryImplHistory extends EntryImpl {
	
	private final CurrentStorage system;
	
	private final String historyId;
	
	EntryImplHistory(final StorageImpl storage, final CurrentStorage system, final BaseLink link, final String historyId) {
		super(storage, system, link);
		this.system = system;
		this.historyId = historyId;
	}
	
	@Override
	public BaseChange createChange() {
		
		return new ChangeForHistory(this.system, this, this.historyId);
	}
	
	@Override
	public String toString() {
		
		return "[object " + this.baseClass() + "(" + "id=" + this.getGuid() + ", historyId=" + this.historyId + ", title=" + this.getTitle() + ")]";
	}
}
