/**
 * 
 */
package ru.myx.xstore.s2;

/**
 * @author myx
 * 
 */
interface ChangeNested {
	/**
	 * @param transaction
	 * @return boolean
	 * @throws Throwable
	 */
	boolean realCommit(final Transaction transaction) throws Throwable;
}
