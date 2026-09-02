/* Rsum.java

 Rsum: A simple, "rolling" checksum based on Adler32
 Copyright (C) 2011 Tomas Hlavnicka <hlavntom@fel.cvut.cz>

 This file is a part of Jazsync.

 Jazsync is free software; you can redistribute it and/or modify it
 under the terms of the GNU General Public License as published by the
 Free Software Foundation; either version 2 of the License, or (at
 your option) any later version.

 Jazsync is distributed in the hope that it will be useful, but
 WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with Jazsync; if not, write to the

 Free Software Foundation, Inc.,
 59 Temple Place, Suite 330,
 Boston, MA  02111-1307
 USA
 */
package org.hashsplit4j.api;

/**
 * Implementation of rolling checksum for zsync purposes
 *
 * @author Tomas Hlavnika
 */
public class Rsum implements Cloneable, java.io.Serializable {

    private short a;
    private short b;
    private int oldByte;
    private int blockLength;
    private byte[] buffer;

    /**
     * Constructor of rolling checksum
     */
    public Rsum(int size) {
        buffer = new byte[size + 1];
        blockLength = size;
        a = b = 0;
        oldByte = 0;
    }

    /**
     * Return the value of the currently computed checksum.
     *
     * @return The currently computed checksum.
     */
    public int getValue() {
        return ((a & 0xffff) | (b << 16));
    }

    /**
     * Reset the checksum
     */
    public void reset() {
        a = b = 0;
        oldByte = 0;
    }

    /**
     * Rolling checksum that takes single byte and compute checksum of block
     * from file in offset that equals offset of newByte minus length of block
     *
     * @param newByte New byte that will actualize a checksum
     */
    public void roll(byte newByte) {
        short oldUnsignedB = unsignedByte(buffer[oldByte]);
        a -= oldUnsignedB;
        b -= blockLength * oldUnsignedB;
        a += unsignedByte(newByte);
        b += a;
        buffer[oldByte] = newByte;
        oldByte++;
        if (oldByte == blockLength) {
            oldByte = 0;
        }
    }


    /**
     * Rolls over a run of bytes, stopping after the one whose checksum matches the mask.
     * <p>
     * Identical to calling {@link #roll(byte)} for each byte and testing {@link #getValue()} after each - the
     * arithmetic below is copied statement for statement, including the implicit narrowing of the compound assignments
     * on the two short accumulators, which is where the wrap-around behaviour every stored hash depends on comes from.
     * <p>
     * What it avoids is per-byte work that has nothing to do with the checksum: a virtual call, and a read and a write
     * of four fields. Held in locals across a run instead, they stay in registers. On a 6.5GB file this is the
     * difference between 33 and 17 seconds of parse time, measured in the Go port of this same algorithm, where the
     * rolling checksum turned out to be 49% of the total - not the SHA-1.
     *
     * @param p bytes to roll over
     * @param off offset into p
     * @param len how many bytes may be rolled
     * @param mask boundary mask; a byte is a boundary when {@code (getValue() & mask) == mask}
     * @return the number of bytes consumed, the last of which is the boundary, or -1 if all len bytes were rolled
     * without finding one
     */
    public int rollBoundary(byte[] p, int off, int len, int mask) {
        // Locals, written back once on the way out.
        short la = a;
        short lb = b;
        int lOldByte = oldByte;
        final int bl = blockLength;
        final byte[] buf = buffer;

        for( int i = 0; i < len; i++ ) {
            byte newByte = p[off + i];
            short oldUnsignedB = unsignedByte(buf[lOldByte]);
            la -= oldUnsignedB;
            lb -= bl * oldUnsignedB;
            la += unsignedByte(newByte);
            lb += la;
            buf[lOldByte] = newByte;
            lOldByte++;
            if( lOldByte == bl ) {
                lOldByte = 0;
            }
            if( ((((la & 0xffff) | (lb << 16))) & mask) == mask ) {
                a = la;
                b = lb;
                oldByte = lOldByte;
                return i + 1;
            }
        }

        a = la;
        b = lb;
        oldByte = lOldByte;
        return -1;
    }

    /**
     * Returns "unsigned" value of byte
     *
     * @param b Byte to convert
     * @return Unsigned value of byte <code>b</code>
     */
    private short unsignedByte(byte b) {
        if (b < 0) {
            return (short) (b + 256);
        }
        return b;
    }

    @Override
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException cnse) {
            throw new Error();
        }
    }

    @Override
    public boolean equals(Object o) {
        return ((Rsum) o).a == a && ((Rsum) o).b == b;
    }
}
