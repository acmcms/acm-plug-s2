/**
 * 
 */
package ru.myx.xstore.s2;

import java.util.function.Function;
import ru.myx.ae3.control.command.ControlCommand;

final class SignalCommand implements Function<Void, Object> {
	private final StorageLevel2		handler;
	
	private final ControlCommand<?>	command;
	
	SignalCommand(final StorageLevel2 handler, final ControlCommand<?> command) {
		this.handler = handler;
		this.command = command;
	}
	
	@Override
	public Object apply(final Void obj) {
		return this.handler.getCommandResult( this.command, null );
	}
	
	@Override
	public String toString() {
		return this.command.getTitle();
	}
}
