/*
 * Created on 30.06.2004
 */
package ru.myx.xstore.s2.jdbc;

import ru.myx.ae1.storage.ModuleSchedule;
import ru.myx.jdbc.lock.Runner;

/**
 * @author myx
 */
final class RunnerScheduling implements Runner {
	private final ModuleSchedule	scheduling;
	
	RunnerScheduling(final ModuleSchedule scheduling) {
		this.scheduling = scheduling;
	}
	
	@Override
	public int getVersion() {
		return 6 + this.scheduling.getVersion();
	}
	
	@Override
	public void start() {
		this.scheduling.start();
	}
	
	@Override
	public void stop() {
		this.scheduling.stop();
	}
}
