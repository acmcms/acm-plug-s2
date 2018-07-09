/*
 * Created on 23.09.2004
 * 
 * Window - Preferences - Java - Code Style - Code Templates
 */
package ru.myx.xstore.s2.local;

import ru.myx.ae3.act.Act;
import ru.myx.ae3.report.Report;

/**
 * @author myx
 */
final class RunnerCleaner implements Runnable {
	private final ExternalizerLocal	externalizer;
	
	private int						leafIndex	= (int) (System.currentTimeMillis() / 1024);
	
	private boolean					started		= false;
	
	RunnerCleaner(final ExternalizerLocal externalizer) {
		this.externalizer = externalizer;
	}
	
	private void clear() throws Throwable {
		this.externalizer.check( this.leafIndex++ );
	}
	
	@Override
	public void run() {
		if (!this.started) {
			return;
		}
		try {
			this.clear();
		} catch (final Throwable t) {
			Report.exception( "S2/LOCAL/CLEANER", "unexpected exception", t );
		}
		if (this.started) {
			Act.later( null, this, 5L * 1000L * 60L );
		}
	}
	
	void start() {
		synchronized (this) {
			if (this.started) {
				return;
			}
			Act.later( null, this, 15L * 1000L * 60L );
			this.started = true;
		}
	}
	
	void stop() {
		synchronized (this) {
			this.started = false;
		}
	}
}
