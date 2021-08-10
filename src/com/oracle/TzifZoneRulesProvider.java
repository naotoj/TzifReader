/*
 * Copyright (c) 2021, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */
package com.oracle;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
import java.time.zone.ZoneRulesException;
import java.time.zone.ZoneRulesProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

// Reads TZif files and parses (RFC8536)
public class TzifZoneRulesProvider extends ZoneRulesProvider {
    private static final int MAGIC = 0x545A6966; // "TZif"

    private final Path ZONEINFODIR = Path.of("/usr/share/zoneinfo");
    private final Map<String, ZoneRules> zoneRulesMap = new TreeMap<>();

    public static void main(String[] args) {
//        new TzifZoneRulesProvider();
        var zoneJP = ZoneId.of("Asia/Tokyo");
    }

    public TzifZoneRulesProvider() {
        try {
            Files.walk(ZONEINFODIR, FileVisitOption.FOLLOW_LINKS)
                    .filter(p -> Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS))
                    .forEach(this::readTZif);
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    private void readTZif(Path p) {
        String zoneId = ZONEINFODIR.relativize(p).toString();
        System.out.println(zoneId);
// temporary
//        if (!p.endsWith("CST6CDT")) {
//        if (!p.endsWith("MST")) {
//            return;
//        }

        try {
            var bb = ByteBuffer.wrap(Files.readAllBytes(p));

//            magic:  The four-octet ASCII [RFC20] sequence "TZif" (0x54 0x5A 0x69
//            0x66), which identifies the file as utilizing the Time Zone
//            Information Format.
            int magic = bb.getInt();
            if (magic != MAGIC) {
                System.out.println("Not a TZif file: " + p);
                return;
            }

//            ver(sion):  An octet identifying the version of the file's format.
//            The value MUST be one of the following:
//
//            NUL (0x00)  Version 1 - The file contains only the version 1
//            header and data block.  Version 1 files MUST NOT contain a
//            version 2+ header, data block, or footer.
//
//            '2' (0x32)  Version 2 - The file MUST contain the version 1 header
//            and data block, a version 2+ header and data block, and a
//            footer.  The TZ string in the footer (Section 3.3), if
//            nonempty, MUST strictly adhere to the requirements for the TZ
//            environment variable as defined in Section 8.3 of the "Base
//            Definitions" volume of [POSIX] and MUST encode the POSIX
//            portable character set as ASCII.
//
//            '3' (0x33)  Version 3 - The file MUST contain the version 1 header
//            and data block, a version 2+ header and data block, and a
//            footer.  The TZ string in the footer (Section 3.3), if
//            nonempty, MUST conform to POSIX requirements with ASCII
//            encoding, except that it MAY use the TZ string extensions
//            described below (Section 3.3.1).
            int ver = switch (bb.get()) {
                case 0 -> 1;
                case '2' -> 2;
                case '3' -> 3;
                default -> throw new InternalError("invalid version");
            };
//            System.out.println("version : " + ver);

            // skip reserved 15 octets
            bb.position(bb.position() + 15);

//                    isutcnt:  A four-octet unsigned integer specifying the number of UT/
//                    local indicators contained in the data block -- MUST either be
//            zero or equal to "typecnt".
//
//                    isstdcnt:  A four-octet unsigned integer specifying the number of
//            standard/wall indicators contained in the data block -- MUST
//            either be zero or equal to "typecnt".
//
//                    leapcnt:  A four-octet unsigned integer specifying the number of
//            leap-second records contained in the data block.
//
//            timecnt:  A four-octet unsigned integer specifying the number of
//            transition times contained in the data block.
//
//                    typecnt:  A four-octet unsigned integer specifying the number of
//            local time type records contained in the data block -- MUST NOT be
//            zero.  (Although local time type records convey no useful
//            information in files that have nonempty TZ strings but no
//            transitions, at least one such record is nevertheless required
//            because many TZif readers reject files that have zero time types.)
//
//                    charcnt:  A four-octet unsigned integer specifying the total number
//            of octets used by the set of time zone designations contained in
//            the data block - MUST NOT be zero.  The count includes the
//            trailing NUL (0x00) octet at the end of the last time zone
//            designation.
            var headerV1 = new Header(bb.getInt(), bb.getInt(), bb.getInt(), bb.getInt(), bb.getInt(), bb.getInt());
//            System.out.println("isUTCount: " + headerV1.isUTCnt+ ", isStdCount: " + headerV1.isStdCnt + ", leapCount: "
//                    + headerV1.leapCnt + ", timeCount: " + headerV1.timeCnt + ", typeCount: " + headerV1.typeCnt
//                    + ", charCount: " + headerV1.charCnt);

            var dataV1 = readDataBlock(1, headerV1, bb);
//            System.out.println(dataV1);

            DataBlock dataV2 = null;
            if (ver > 1) {
                // skip magic/ver/reserved 20 octets
                bb.position(bb.position() + 20);
                var headerV2 = new Header(bb.getInt(), bb.getInt(), bb.getInt(), bb.getInt(), bb.getInt(), bb.getInt());
//                System.out.println("isUTCount: " + headerV2.isUTCnt+ ", isStdCount: " + headerV2.isStdCnt + ", leapCount: "
//                        + headerV2.leapCnt + ", timeCount: " + headerV2.timeCnt + ", typeCount: " + headerV2.typeCnt
//                        + ", charCount: " + headerV2.charCnt);

                dataV2 = readDataBlock(ver, headerV2, bb);
//                System.out.println(dataV2);

                // Footer
                var NL = bb.get(); // NL
                assert NL == '\n';
                var sb = new StringBuilder();
                for (char c = (char)bb.get(); c != '\n'; c = (char)bb.get()){
                    sb.append(c);
                }
//                System.out.println(sb);

            }
            var zr = createRules(dataV2 != null ? dataV2 : dataV1);
            zoneRulesMap.put(zoneId, zr);
//            compareTransitions(zr.getTransitions(), zoneId);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private static DataBlock readDataBlock(int ver, Header header, ByteBuffer bb) {
//        transition times:  A series of four- or eight-octet UNIX leap-time
//        values sorted in strictly ascending order.  Each value is used as
//        a transition time at which the rules for computing local time may
//        change.  The number of time values is specified by the "timecnt"
//        field in the header.  Each time value SHOULD be at least -2**59.
//        (-2**59 is the greatest negated power of 2 that predates the Big
//        Bang, and avoiding earlier timestamps works around known TZif
//        reader bugs relating to outlandishly negative timestamps.)
        var transTimes = new Instant[header.timeCnt];
        for (int i = 0; i < header.timeCnt; i++) {
            var unixTime = (ver == 1 ? bb.getInt() : bb.getLong());
            transTimes[i] = Instant.ofEpochSecond(unixTime);
        }

//        transition types:  A series of one-octet unsigned integers specifying
//        the type of local time of the corresponding transition time.
//        These values serve as zero-based indices into the array of local
//        time type records.  The number of type indices is specified by the
//        "timecnt" field in the header.  Each type index MUST be in the
//        range [0, "typecnt" - 1].
        var transTypes = new byte[header.timeCnt];
        for (int i = 0; i < header.timeCnt; i++) {
            transTypes[i] = bb.get();
        }

//        local time type records:  A series of six-octet records specifying a
//        local time type.  The number of records is specified by the
//        "typecnt" field in the header.  Each record has the following
//        format (the lengths of multi-octet fields are shown in
//                parentheses):
//
//        +---------------+---+---+
//        |  utoff (4)    |dst|idx|
//        +---------------+---+---+
        var localTimeTypeRecords = new LocalTimeTypeRecord[header.typeCnt];
        for (int i = 0; i < header.typeCnt; i++) {
            localTimeTypeRecords[i] = new LocalTimeTypeRecord(bb.getInt(), bb.get(), bb.get());
        }
        var timeZoneDesig = new byte[header.charCnt];
        for (int i = 0; i < header.charCnt; i++) {
            timeZoneDesig[i] = bb.get();
        }

//        leap-second records:  A series of eight- or twelve-octet records
//        specifying the corrections that need to be applied to UTC in order
//        to determine TAI.  The records are sorted by the occurrence time
//        in strictly ascending order.  The number of records is specified
//        by the "leapcnt" field in the header.  Each record has one of the
//        following structures (the lengths of multi-octet fields are shown
//        in parentheses):
//
//        Version 1 Data Block:
//
//        +---------------+---------------+
//        |  occur (4)    |  corr (4)     |
//        +---------------+---------------+
//
//        version 2+ Data Block:
//
//        +---------------+---------------+---------------+
//        |  occur (8)                    |  corr (4)     |
//        +---------------+---------------+---------------+
//
        var leapSecondRecords = new LeapSecondRecord[header.leapCnt];
        for (int i = 0; i < header.leapCnt; i++) {
            leapSecondRecords[i] = new LeapSecondRecord(ver == 1 ? bb.getInt() : bb.getLong(), bb.getInt());
        }

//        standard/wall indicators:  A series of one-octet values indicating
//        whether the transition times associated with local time types were
//        specified as standard time or wall-clock time.  Each value MUST be
//        0 or 1.  A value of one (1) indicates standard time.  The value
//        MUST be set to one (1) if the corresponding UT/local indicator is
//        set to one (1).  A value of zero (0) indicates wall time.  The
//        number of values is specified by the "isstdcnt" field in the
//        header.  If "isstdcnt" is zero (0), all transition times
//        associated with local time types are assumed to be specified as
//        wall time.
        var isStds = new boolean[header.isStdCnt];
        for (int i = 0; i < header.isStdCnt; i++) {
            isStds[i] = bb.get() != 0;
        }

//        UT/local indicators:  A series of one-octet values indicating whether
//        the transition times associated with local time types were
//        specified as UT or local time.  Each value MUST be 0 or 1.  A
//        value of one (1) indicates UT, and the corresponding standard/wall
//        indicator MUST also be set to one (1).  A value of zero (0)
//        indicates local time.  The number of values is specified by the
//        "isutcnt" field in the header.  If "isutcnt" is zero (0), all
//        transition times associated with local time types are assumed to
//        be specified as local time.
        var isUTs = new boolean[header.isUTCnt];
        for (int i = 0; i < header.isUTCnt; i++) {
            isUTs[i] = bb.get() != 0;
        }

        return new DataBlock(transTimes, transTypes, localTimeTypeRecords, timeZoneDesig, leapSecondRecords, isStds, isUTs);
    }

    static record Header(int isUTCnt,
                         int isStdCnt,
                         int leapCnt,
                         int timeCnt,
                         int typeCnt,
                         int charCnt) {}
    static record DataBlock(Instant[] transitionTimes,
                            byte[] transitionTypes,
                            LocalTimeTypeRecord[] localTimeTypeRecords,
                            byte[] timeZoneDesignations,
                            LeapSecondRecord[] leapSecondRecords,
                            boolean[] isStandard,
                            boolean[] isUT) {}
//    utoff:  A four-octet signed integer specifying the number of
//    seconds to be added to UT in order to determine local time.
//    The value MUST NOT be -2**31 and SHOULD be in the range
//         [-89999, 93599] (i.e., its value SHOULD be more than -25 hours
//    and less than 26 hours).  Avoiding -2**31 allows 32-bit clients
//    to negate the value without overflow.  Restricting it to
//         [-89999, 93599] allows easy support by implementations that
//    already support the POSIX-required range [-24:59:59, 25:59:59].
//
//            (is)dst:  A one-octet value indicating whether local time should
//    be considered Daylight Saving Time (DST).  The value MUST be 0
//    or 1.  A value of one (1) indicates that this type of time is
//    DST.  A value of zero (0) indicates that this time type is
//    standard time.
//
//            (desig)idx:  A one-octet unsigned integer specifying a zero-based
//    index into the series of time zone designation octets, thereby
//    selecting a particular designation string.  Each index MUST be
//    in the range [0, "charcnt" - 1]; it designates the
//    NUL-terminated string of octets starting at position "idx" in
//    the time zone designations.  (This string MAY be empty.)  A NUL
//    octet MUST exist in the time zone designations at or after
//    position "idx".
    static record LocalTimeTypeRecord(int utoff, byte dst, byte idx) {}

//    occur(rence):  A four- or eight-octet UNIX leap time value
//    specifying the time at which a leap-second correction occurs.
//    The first value, if present, MUST be nonnegative, and each
//    later value MUST be at least 2419199 greater than the previous
//         value.  (This is 28 days' worth of seconds, minus a potential
//    negative leap second.)
//
//    corr(ection):  A four-octet signed integer specifying the value of
//    LEAPCORR on or after the occurrence.  The correction value in
//    the first leap-second record, if present, MUST be either one
//            (1) or minus one (-1).  The correction values in adjacent leap-
//    second records MUST differ by exactly one (1).  The value of
//    LEAPCORR is zero for timestamps that occur before the
//    occurrence time in the first leap-second record (or for all
//            timestamps if there are no leap-second records).
    static record LeapSecondRecord(long occur, int corr) {}


//    private static ZoneRules createRules(DataBlock db) {
    private static ZoneRules createRules(DataBlock db) {
        // check for fixed offsets
        if (db.transitionTypes.length == 0) {
            return ZoneRules.of(ZoneOffset.ofTotalSeconds(db.localTimeTypeRecords[0].utoff));
        }

        var transitions = new ArrayList<ZoneOffsetTransition>();
        var stdTransitions = new ArrayList<ZoneOffsetTransition>();
        // look for the first offsets
        ZoneOffset firstStdOff = null;
        ZoneOffset stdOff = null;
        ZoneOffset firstWallOff = ZoneOffset.ofTotalSeconds(db.localTimeTypeRecords[0].utoff);
        for (int i = 0; i < db.localTimeTypeRecords.length; i ++) {
            if (db.localTimeTypeRecords[i].dst == 0) {
                stdOff = firstStdOff = ZoneOffset.ofTotalSeconds(db.localTimeTypeRecords[i].utoff);
                break;
            }
        }
        var before = firstWallOff;
        var after = before;
        for (int i = 0; i < db.transitionTimes.length; i++) {
            int utoff = db.localTimeTypeRecords[db.transitionTypes[i]].utoff;
            after = ZoneOffset.ofTotalSeconds(utoff);

            // std transitions
            if (db.localTimeTypeRecords[db.transitionTypes[i]].dst == 0 && !stdOff.equals(after)) {
                stdTransitions.add(
                   ZoneOffsetTransition.of(
                        LocalDateTime.ofInstant(db.transitionTimes[i].plusSeconds(stdOff.getTotalSeconds() - utoff), after),
                        stdOff, after));
                stdOff = after;
            }

            // wall transitions
            if (!before.equals(after)) {
                transitions.add(
                    ZoneOffsetTransition.of(
                        LocalDateTime.ofInstant(db.transitionTimes[i].plusSeconds(before.getTotalSeconds() - utoff), after),
                        before, after));
                before = after;
            }
        }
        return ZoneRules.of(firstStdOff, firstWallOff, stdTransitions, transitions, List.of());
    }

    // compare transitions
    private static void compareTransitions(List<ZoneOffsetTransition> got, String refZone) {
        try {
            var zot = ZoneId.of(refZone).getRules().getTransitions();
            for (int i = 0; i < Math.min(got.size(), zot.size()); i++) {
                var g = got.get(i);
                var r = zot.get(i);
                if (!g.equals(r)) {
                    throw new RuntimeException("" + i + ": got: " + g + ", r: " + r);
                }
            }
        } catch (ZoneRulesException zre) {
            System.out.println("compare ignored. ZRE thrown for " + refZone);
        }
    }

    // ZoneRulesProvider methods implementations
    @Override
    protected Set<String> provideZoneIds() {
        return zoneRulesMap.keySet();
    }

    @Override
    protected ZoneRules provideRules(String zoneId, boolean forCaching) {
        return zoneRulesMap.get(zoneId);
    }

    @Override
    protected NavigableMap<String, ZoneRules> provideVersions(String zoneId) {
        return new TreeMap<>(Map.of("v1", zoneRulesMap.get(zoneId)));
    }
}
