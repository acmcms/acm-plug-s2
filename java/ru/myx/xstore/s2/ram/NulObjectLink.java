/*
 * Created on 28.12.2005
 */
package ru.myx.xstore.s2.ram;

import java.util.Map;

import ru.myx.ae1.storage.BaseHistory;
import ru.myx.ae3.base.BaseObject;
import ru.myx.xstore.s2.AbstractLink;
import ru.myx.xstore.s2.BaseLink;

final class NulObjectLink extends AbstractLink {
	@Override
	public String getChildByName(final String name) {
		return null;
	}
	
	@Override
	public String[] getChildren(final int count, final String sort) {
		return null;
	}
	
	@Override
	public String[] getChildrenListable(final int count, final String sort) {
		return null;
	}
	
	@Override
	public long getCreated() {
		return 0;
	}
	
	@Override
	public BaseObject getData() {
		return null;
	}
	
	@Override
	public String[] getFiles(final int count, final String sort) {
		return null;
	}
	
	@Override
	public String[] getFilesListable(final int count, final String sort) {
		return null;
	}
	
	@Override
	public String[] getFolders(final int count, final String sort) {
		return null;
	}
	
	@Override
	public String[] getFoldersListable() {
		return null;
	}
	
	@Override
	public String getGuid() {
		return null;
	}
	
	@Override
	public BaseHistory[] getHistory() {
		return null;
	}
	
	@Override
	public BaseLink getHistorySnapshot(final String historyId) {
		return null;
	}
	
	@Override
	public String getKey() {
		return null;
	}
	
	@Override
	public String getLinkedIdentity() {
		return null;
	}
	
	@Override
	public long getModified() {
		return 0;
	}
	
	@Override
	public String getOwner() {
		return null;
	}
	
	@Override
	public String getParentGuid() {
		return null;
	}
	
	@Override
	public int getState() {
		return 0;
	}
	
	@Override
	public String getTitle() {
		return null;
	}
	
	@Override
	public String getTypeName() {
		return null;
	}
	
	@Override
	public boolean isFolder() {
		return false;
	}
	
	@Override
	public Map.Entry<String, Object>[] search(
			final int limit,
			final boolean all,
			final long timeout,
			final String sort,
			final long dateStart,
			final long dateEnd,
			final String filter) {
		return null;
	}
	
	@Override
	public Map<Integer, Map<Integer, Map<String, Number>>> searchCalendar(
			final boolean all,
			final long timeout,
			final long startDate,
			final long endDate,
			final String filter) {
		return null;
	}
	
	@Override
	public String[] searchLocal(
			final int limit,
			final boolean all,
			final String sort,
			final long startDate,
			final long endDate,
			final String query) {
		return null;
	}
	
	@Override
	public Map<String, Number> searchLocalAlphabet(
			final boolean all,
			final Map<String, String> alphabetConversion,
			final String defaultLetter,
			final String filter) {
		return null;
	}
	
	@Override
	public String[] searchLocalAlphabet(
			final int limit,
			final boolean all,
			final String sort,
			final Map<String, String> alphabetConversion,
			final String defaultLetter,
			final String filterLetter,
			final String query) {
		return null;
	}
	
	@Override
	public Map<Integer, Map<Integer, Map<String, Number>>> searchLocalCalendar(
			final boolean all,
			final long startDate,
			final long endDate,
			final String query) {
		return null;
	}
}
