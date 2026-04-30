// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

contract Geohash {
    uint8 public constant LEAF_GEOHASH_DEPTH = 7;

    event RegisterBatchGasBreakdown(
        uint256 indexed batchId,
        uint256 setupGas,
        uint256 spatialGas,
        uint256 temporalGas,
        uint256 sharedGas
    );

    struct Batch {
        uint256 batchId;
        bytes wkb;
        uint64 timestamp;
        uint256 lotId;
    }

    struct BatchRef {
        uint192 batchId;
        uint64 timestamp;
    }

    struct Parcel {
        uint256 parcelId;
        bytes wkb;
        bool exists;
    }

    uint256 public batchCount;
    uint256 public parcelCount;

    mapping(uint256 => Batch) public batches;
    mapping(uint256 => Parcel) public parcels;

    mapping(uint256 => bytes7) public batchToSpatialLeaf;
    mapping(uint256 => uint256) public batchToTimeBucket;

    mapping(bytes4 => bytes5[]) internal level4Children;
    mapping(bytes5 => bytes6[]) internal level5Children;
    mapping(bytes6 => bytes7[]) internal level6Children;

    mapping(bytes4 => mapping(bytes5 => bool)) internal hasLevel4Child;
    mapping(bytes5 => mapping(bytes6 => bool)) internal hasLevel5Child;
    mapping(bytes6 => mapping(bytes7 => bool)) internal hasLevel6Child;

    mapping(bytes7 => BatchRef[]) internal spatial7;

    mapping(uint256 => bool) public timeBucketExists;
    uint256[] internal timeBuckets;

    mapping(uint256 => bytes4[]) internal temporalRoots4;
    mapping(uint256 => mapping(bytes4 => bool)) internal hasTemporalRoot4;

    mapping(uint256 => mapping(bytes4 => bytes5[])) internal temporalLevel4Children;
    mapping(uint256 => mapping(bytes5 => bytes6[])) internal temporalLevel5Children;
    mapping(uint256 => mapping(bytes6 => bytes7[])) internal temporalLevel6Children;

    mapping(uint256 => mapping(bytes4 => mapping(bytes5 => bool))) internal hasTemporalLevel4Child;
    mapping(uint256 => mapping(bytes5 => mapping(bytes6 => bool))) internal hasTemporalLevel5Child;
    mapping(uint256 => mapping(bytes6 => mapping(bytes7 => bool))) internal hasTemporalLevel6Child;

    mapping(uint256 => mapping(bytes7 => uint256[])) internal temporal7;

    mapping(uint256 => bytes7[]) internal parcelLeaves;
    mapping(uint256 => mapping(bytes7 => bool)) internal parcelHasLeaf;

    mapping(bytes7 => uint256[]) internal leafToParcels;
    mapping(bytes7 => mapping(uint256 => bool)) internal leafHasParcel;

    struct Lot {
        uint256 lotId;
        uint256 parcelId;
        bytes wkb;
        bool exists;
    }

    uint256 public lotCount;
    mapping(uint256 => Lot) public lots;
    mapping(uint256 => uint256[]) internal lotBatchHistory;
    mapping(uint256 => uint256[]) internal parcelLots;

    // ═══════════════════════════════════════════════════════════════
    // WRITE FUNCTIONS
    // ═══════════════════════════════════════════════════════════════

    function registerLot(
        uint256 parcelId,
        bytes calldata wkb
    ) external returns (uint256 lotId) {
        require(parcels[parcelId].exists, "parcel not found");
        lotId = ++lotCount;
        lots[lotId] = Lot(lotId, parcelId, wkb, true);
        parcelLots[parcelId].push(lotId);
    }

    function registerBatch(
        uint256 lotId,
        bytes calldata wkb,
        string calldata geohash,
        uint64 timestamp
    ) external returns (uint256 batchId) {
        (batchId, , , , ) = _registerBatch(lotId, wkb, geohash, timestamp, false);
    }

    function registerBatchMeasured(
        uint256 lotId,
        bytes calldata wkb,
        string calldata geohash,
        uint64 timestamp
    ) external returns (uint256 batchId) {
        uint256 setupGas;
        uint256 spatialGas;
        uint256 temporalGas;
        uint256 sharedGas;

        (batchId, setupGas, spatialGas, temporalGas, sharedGas) =
            _registerBatch(lotId, wkb, geohash, timestamp, true);

        emit RegisterBatchGasBreakdown(
            batchId,
            setupGas,
            spatialGas,
            temporalGas,
            sharedGas
        );
    }

    function registerParcel(
        bytes calldata wkb,
        bytes7[] calldata coveredLeaves
    ) external returns (uint256 parcelId) {
        require(coveredLeaves.length > 0, "empty coverage");
        parcelId = ++parcelCount;
        parcels[parcelId] = Parcel(parcelId, wkb, true);

        for (uint256 i; i < coveredLeaves.length;) {
            bytes7 leaf = coveredLeaves[i];
            if (!parcelHasLeaf[parcelId][leaf]) {
                parcelHasLeaf[parcelId][leaf] = true;
                parcelLeaves[parcelId].push(leaf);
            }
            if (!leafHasParcel[leaf][parcelId]) {
                leafHasParcel[leaf][parcelId] = true;
                leafToParcels[leaf].push(parcelId);
            }
            unchecked { i++; }
        }
    }

    function _registerBatch(
        uint256 lotId,
        bytes calldata wkb,
        string calldata geohash,
        uint64 timestamp,
        bool measure
    )
        internal
        returns (
            uint256 batchId,
            uint256 setupGas,
            uint256 spatialGas,
            uint256 temporalGas,
            uint256 sharedGas
        )
    {
        require(lots[lotId].exists, "lot not found");

        uint256 gasStart;
        if (measure) {
            gasStart = gasleft();
        }

        bytes7 p7 = _validatedLeafGeohash(geohash);
        bytes6 p6 = bytes6(p7);
        bytes5 p5 = bytes5(p7);
        bytes4 p4 = bytes4(p7);

        batchId = ++batchCount;
        uint256 timeBucket = getTimeBucket(timestamp);

        if (measure) {
            setupGas = gasStart - gasleft();
            gasStart = gasleft();
        }

        _ensureTimeBucket(timeBucket);

        if (measure) {
            temporalGas = gasStart - gasleft();
            gasStart = gasleft();
        }

        batches[batchId] = Batch(batchId, wkb, timestamp, lotId);

        if (measure) {
            setupGas += gasStart - gasleft();
            gasStart = gasleft();
        }

        _link45(p4, p5);
        _link56(p5, p6);
        _link67(p6, p7);
        spatial7[p7].push(BatchRef(uint192(batchId), timestamp));

        if (measure) {
            spatialGas = gasStart - gasleft();
            gasStart = gasleft();
        }

        _linkTemporalRoot4(timeBucket, p4);
        _linkTemporal45(timeBucket, p4, p5);
        _linkTemporal56(timeBucket, p5, p6);
        _linkTemporal67(timeBucket, p6, p7);
        temporal7[timeBucket][p7].push(batchId);

        if (measure) {
            temporalGas += gasStart - gasleft();
            gasStart = gasleft();
        }

        batchToSpatialLeaf[batchId] = p7;
        batchToTimeBucket[batchId] = timeBucket;
        lotBatchHistory[lotId].push(batchId);

        if (measure) {
            sharedGas = gasStart - gasleft();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // QUERIES
    // ═══════════════════════════════════════════════════════════════

    // Q1: all batches in a geographic area
    function findBatchesInArea(bytes4 prefix4)
        external view returns (uint256[] memory)
    {
        (uint256[] memory result, uint256 count) = _collectSpatialBatches(prefix4);
        return _trim(result, count);
    }

    // Q2: all batches in a time period
    function findBatchesInTimePeriod(uint256 startBucket, uint256 endBucket)
        external view returns (uint256[] memory)
    {
        (uint256[] memory result, uint256 count) = _collectTemporalBatches(startBucket, endBucket);
        return _trim(result, count);
    }

    // Q3: batches in area during time — SPATIAL-FIRST
    function findBatchesInAreaDuringTime_SpatialFirst(
        bytes4 prefix4,
        uint256 startBucket,
        uint256 endBucket
    ) external view returns (uint256[] memory) {
        (uint256[] memory spatialAll, uint256 spatialCount) = _collectSpatialBatches(prefix4);
        uint256[] memory temp = new uint256[](spatialCount);
        uint256 idx;
        for (uint256 i; i < spatialCount;) {
            uint256 tb = batchToTimeBucket[spatialAll[i]];
            if (tb >= startBucket && tb <= endBucket) {
                temp[idx] = spatialAll[i];
                unchecked { idx++; }
            }
            unchecked { i++; }
        }
        return _trim(temp, idx);
    }

    // Q4: batches in area during time — TEMPORAL-FIRST
    function findBatchesInAreaDuringTime_TemporalFirst(
        bytes4 prefix4,
        uint256 startBucket,
        uint256 endBucket
    ) external view returns (uint256[] memory) {
        (uint256[] memory result, uint256 count) = _collectTemporalScoped(prefix4, startBucket, endBucket);
        return _trim(result, count);
    }

    // Q5: all batches inside a parcel
    function findBatchesInsideParcel(uint256 parcelId)
        external view returns (uint256[] memory)
    {
        uint256 total = _countParcelBatches(parcelId);
        uint256[] memory result = new uint256[](total);
        uint256 idx;
        bytes7[] storage leaves = parcelLeaves[parcelId];
        for (uint256 i; i < leaves.length;) {
            idx = _fillFromLeaf(leaves[i], result, idx);
            unchecked { i++; }
        }
        return result;
    }

    // Q6: batches inside a parcel during time
    function findBatchesInsideParcelDuringTime(
        uint256 parcelId,
        uint256 startBucket,
        uint256 endBucket
    ) external view returns (uint256[] memory) {
        uint256 cap = _countParcelBatches(parcelId);
        uint256[] memory temp = new uint256[](cap);
        uint256 idx;
        bytes7[] storage leaves = parcelLeaves[parcelId];
        for (uint256 i; i < leaves.length;) {
            idx = _filterLeafByTime(leaves[i], startBucket, endBucket, temp, idx);
            unchecked { i++; }
        }
        return _trim(temp, idx);
    }

    // Q7: count batches in area
    function countBatchesInArea(bytes4 prefix4)
        external view returns (uint256)
    {
        return _countSpatialBatches(prefix4);
    }

    // Q8: all lots that have batches in an area
    function findLotsInArea(bytes4 prefix4)
        external view returns (uint256[] memory)
    {
        (uint256[] memory allBatches, uint256 count) = _collectSpatialBatches(prefix4);
        return _extractUniqueLots(allBatches, count);
    }

    // Q9: full trajectory of a lot
    function getLotTrajectory(uint256 lotId)
        external view returns (
            uint256[] memory batchIds,
            bytes7[] memory locations,
            uint64[] memory timestamps
        )
    {
        uint256[] storage history = lotBatchHistory[lotId];
        uint256 len = history.length;
        batchIds = new uint256[](len);
        locations = new bytes7[](len);
        timestamps = new uint64[](len);
        for (uint256 i; i < len;) {
            uint256 bid = history[i];
            batchIds[i] = bid;
            locations[i] = batchToSpatialLeaf[bid];
            timestamps[i] = batches[bid].timestamp;
            unchecked { i++; }
        }
    }

    // Q10: lot location at a specific time
    function getLotLocationAtTime(uint256 lotId, uint64 targetTime)
        external view returns (uint256, bytes7, uint64, bool)
    {
        uint256[] storage history = lotBatchHistory[lotId];
        for (uint256 i = history.length; i > 0;) {
            unchecked { i--; }
            uint256 bid = history[i];
            uint64 ts = batches[bid].timestamp;
            if (ts <= targetTime) {
                return (bid, batchToSpatialLeaf[bid], ts, true);
            }
        }
        return (0, bytes7(0), 0, false);
    }

    // Q11: parcels at a leaf location
    function findParcelsAtLocation(bytes7 leaf)
        external view returns (uint256[] memory)
    {
        return leafToParcels[leaf];
    }

    // Q12: lots inside a parcel during a time window
    function findLotsInsideParcelDuringTime(
        uint256 parcelId,
        uint256 startBucket,
        uint256 endBucket
    ) external view returns (uint256[] memory) {
        uint256 cap = _countParcelBatches(parcelId);
        uint256[] memory temp = new uint256[](cap);
        uint256 idx;
        bytes7[] storage leaves = parcelLeaves[parcelId];
        for (uint256 i; i < leaves.length;) {
            idx = _filterLeafByTime(leaves[i], startBucket, endBucket, temp, idx);
            unchecked { i++; }
        }
        return _extractUniqueLots(temp, idx);
    }

    // ═══════════════════════════════════════════════════════════════
    // SPATIAL TREE HELPERS
    // ═══════════════════════════════════════════════════════════════

    function _countSpatialBatches(bytes4 prefix4)
        internal view returns (uint256 total)
    {
        bytes5[] storage c5 = level4Children[prefix4];
        for (uint256 i; i < c5.length;) {
            total += _countSpatialL5(c5[i]);
            unchecked { i++; }
        }
    }

    function _countSpatialL5(bytes5 p5)
        internal view returns (uint256 total)
    {
        bytes6[] storage c6 = level5Children[p5];
        for (uint256 j; j < c6.length;) {
            total += _countSpatialL6(c6[j]);
            unchecked { j++; }
        }
    }

    function _countSpatialL6(bytes6 p6)
        internal view returns (uint256 total)
    {
        bytes7[] storage c7 = level6Children[p6];
        for (uint256 k; k < c7.length;) {
            total += spatial7[c7[k]].length;
            unchecked { k++; }
        }
    }

    function _collectSpatialBatches(bytes4 prefix4)
        internal view returns (uint256[] memory, uint256)
    {
        uint256 cap = _countSpatialBatches(prefix4);
        uint256[] memory result = new uint256[](cap);
        uint256 idx;
        bytes5[] storage c5 = level4Children[prefix4];
        for (uint256 i; i < c5.length;) {
            idx = _fillSpatialL5(c5[i], result, idx);
            unchecked { i++; }
        }
        return (result, idx);
    }

    function _fillSpatialL5(bytes5 p5, uint256[] memory result, uint256 idx)
        internal view returns (uint256)
    {
        bytes6[] storage c6 = level5Children[p5];
        for (uint256 j; j < c6.length;) {
            idx = _fillSpatialL6(c6[j], result, idx);
            unchecked { j++; }
        }
        return idx;
    }

    function _fillSpatialL6(bytes6 p6, uint256[] memory result, uint256 idx)
        internal view returns (uint256)
    {
        bytes7[] storage c7 = level6Children[p6];
        for (uint256 k; k < c7.length;) {
            BatchRef[] storage refs = spatial7[c7[k]];
            for (uint256 m; m < refs.length;) {
                result[idx] = uint256(refs[m].batchId);
                unchecked { idx++; m++; }
            }
            unchecked { k++; }
        }
        return idx;
    }

    // ═══════════════════════════════════════════════════════════════
    // TEMPORAL TREE HELPERS
    // ═══════════════════════════════════════════════════════════════

    function _countTemporalBatches(uint256 startBucket, uint256 endBucket)
        internal view returns (uint256 total)
    {
        for (uint256 tb = startBucket; tb <= endBucket;) {
            if (timeBucketExists[tb]) {
                bytes4[] storage roots = temporalRoots4[tb];
                for (uint256 i; i < roots.length;) {
                    total += _countTemporalL4(tb, roots[i]);
                    unchecked { i++; }
                }
            }
            unchecked { tb++; }
        }
    }

    function _countTemporalL4(uint256 tb, bytes4 p4)
        internal view returns (uint256 total)
    {
        bytes5[] storage c5 = temporalLevel4Children[tb][p4];
        for (uint256 j; j < c5.length;) {
            total += _countTemporalL5(tb, c5[j]);
            unchecked { j++; }
        }
    }

    function _countTemporalL5(uint256 tb, bytes5 p5)
        internal view returns (uint256 total)
    {
        bytes6[] storage c6 = temporalLevel5Children[tb][p5];
        for (uint256 k; k < c6.length;) {
            total += _countTemporalL6(tb, c6[k]);
            unchecked { k++; }
        }
    }

    function _countTemporalL6(uint256 tb, bytes6 p6)
        internal view returns (uint256 total)
    {
        bytes7[] storage c7 = temporalLevel6Children[tb][p6];
        for (uint256 m; m < c7.length;) {
            total += temporal7[tb][c7[m]].length;
            unchecked { m++; }
        }
    }

    function _collectTemporalBatches(uint256 startBucket, uint256 endBucket)
        internal view returns (uint256[] memory, uint256)
    {
        uint256 cap = _countTemporalBatches(startBucket, endBucket);
        uint256[] memory result = new uint256[](cap);
        uint256 idx;
        for (uint256 tb = startBucket; tb <= endBucket;) {
            if (timeBucketExists[tb]) {
                bytes4[] storage roots = temporalRoots4[tb];
                for (uint256 i; i < roots.length;) {
                    idx = _fillTemporalL4(tb, roots[i], result, idx);
                    unchecked { i++; }
                }
            }
            unchecked { tb++; }
        }
        return (result, idx);
    }

    function _collectTemporalScoped(bytes4 prefix4, uint256 startBucket, uint256 endBucket)
        internal view returns (uint256[] memory, uint256)
    {
        uint256 cap = _countTemporalScoped(prefix4, startBucket, endBucket);
        uint256[] memory result = new uint256[](cap);
        uint256 idx;
        for (uint256 tb = startBucket; tb <= endBucket;) {
            if (timeBucketExists[tb] && hasTemporalRoot4[tb][prefix4]) {
                idx = _fillTemporalL4(tb, prefix4, result, idx);
            }
            unchecked { tb++; }
        }
        return (result, idx);
    }

    function _countTemporalScoped(bytes4 prefix4, uint256 startBucket, uint256 endBucket)
        internal view returns (uint256 total)
    {
        for (uint256 tb = startBucket; tb <= endBucket;) {
            if (timeBucketExists[tb] && hasTemporalRoot4[tb][prefix4]) {
                total += _countTemporalL4(tb, prefix4);
            }
            unchecked { tb++; }
        }
    }

    function _fillTemporalL4(uint256 tb, bytes4 p4, uint256[] memory result, uint256 idx)
        internal view returns (uint256)
    {
        bytes5[] storage c5 = temporalLevel4Children[tb][p4];
        for (uint256 j; j < c5.length;) {
            idx = _fillTemporalL5(tb, c5[j], result, idx);
            unchecked { j++; }
        }
        return idx;
    }

    function _fillTemporalL5(uint256 tb, bytes5 p5, uint256[] memory result, uint256 idx)
        internal view returns (uint256)
    {
        bytes6[] storage c6 = temporalLevel5Children[tb][p5];
        for (uint256 k; k < c6.length;) {
            idx = _fillTemporalL6(tb, c6[k], result, idx);
            unchecked { k++; }
        }
        return idx;
    }

    function _fillTemporalL6(uint256 tb, bytes6 p6, uint256[] memory result, uint256 idx)
        internal view returns (uint256)
    {
        bytes7[] storage c7 = temporalLevel6Children[tb][p6];
        for (uint256 m; m < c7.length;) {
            uint256[] storage ids = temporal7[tb][c7[m]];
            for (uint256 n; n < ids.length;) {
                result[idx] = ids[n];
                unchecked { idx++; n++; }
            }
            unchecked { m++; }
        }
        return idx;
    }

    // ═══════════════════════════════════════════════════════════════
    // PARCEL + SHARED HELPERS
    // ═══════════════════════════════════════════════════════════════

    function _countParcelBatches(uint256 parcelId)
        internal view returns (uint256 total)
    {
        bytes7[] storage leaves = parcelLeaves[parcelId];
        for (uint256 i; i < leaves.length;) {
            total += spatial7[leaves[i]].length;
            unchecked { i++; }
        }
    }

    function _fillFromLeaf(bytes7 leaf, uint256[] memory result, uint256 idx)
        internal view returns (uint256)
    {
        BatchRef[] storage refs = spatial7[leaf];
        for (uint256 j; j < refs.length;) {
            result[idx] = uint256(refs[j].batchId);
            unchecked { idx++; j++; }
        }
        return idx;
    }

    function _filterLeafByTime(
        bytes7 leaf,
        uint256 startBucket,
        uint256 endBucket,
        uint256[] memory result,
        uint256 idx
    ) internal view returns (uint256) {
        BatchRef[] storage refs = spatial7[leaf];
        for (uint256 j; j < refs.length;) {
            uint256 bid = uint256(refs[j].batchId);
            uint256 tb = batchToTimeBucket[bid];
            if (tb >= startBucket && tb <= endBucket) {
                result[idx] = bid;
                unchecked { idx++; }
            }
            unchecked { j++; }
        }
        return idx;
    }

    function _extractUniqueLots(uint256[] memory batchIds, uint256 count)
        internal view returns (uint256[] memory)
    {
        uint256[] memory temp = new uint256[](count);
        uint256 uniqueCount;
        for (uint256 i; i < count;) {
            uint256 lid = batches[batchIds[i]].lotId;
            bool found;
            for (uint256 j; j < uniqueCount;) {
                if (temp[j] == lid) { found = true; break; }
                unchecked { j++; }
            }
            if (!found) {
                temp[uniqueCount] = lid;
                unchecked { uniqueCount++; }
            }
            unchecked { i++; }
        }
        return _trim(temp, uniqueCount);
    }

    function _trim(uint256[] memory arr, uint256 len)
        internal pure returns (uint256[] memory)
    {
        if (arr.length == len) return arr;
        uint256[] memory trimmed = new uint256[](len);
        for (uint256 i; i < len;) {
            trimmed[i] = arr[i];
            unchecked { i++; }
        }
        return trimmed;
    }

    // ═══════════════════════════════════════════════════════════════
    // TREE LINK + UTILITY FUNCTIONS
    // ═══════════════════════════════════════════════════════════════

    function getTimeBucket(uint64 timestamp) public pure returns (uint256) {
        return uint256(timestamp / 1 days);
    }

    function _ensureTimeBucket(uint256 timeBucket) internal {
        if (!timeBucketExists[timeBucket]) {
            timeBucketExists[timeBucket] = true;
            timeBuckets.push(timeBucket);
        }
    }

    function _link45(bytes4 parent, bytes5 child) internal {
        if (!hasLevel4Child[parent][child]) {
            hasLevel4Child[parent][child] = true;
            level4Children[parent].push(child);
        }
    }

    function _link56(bytes5 parent, bytes6 child) internal {
        if (!hasLevel5Child[parent][child]) {
            hasLevel5Child[parent][child] = true;
            level5Children[parent].push(child);
        }
    }

    function _link67(bytes6 parent, bytes7 child) internal {
        if (!hasLevel6Child[parent][child]) {
            hasLevel6Child[parent][child] = true;
            level6Children[parent].push(child);
        }
    }

    function _linkTemporalRoot4(uint256 timeBucket, bytes4 root4) internal {
        if (!hasTemporalRoot4[timeBucket][root4]) {
            hasTemporalRoot4[timeBucket][root4] = true;
            temporalRoots4[timeBucket].push(root4);
        }
    }

    function _linkTemporal45(uint256 timeBucket, bytes4 parent, bytes5 child) internal {
        if (!hasTemporalLevel4Child[timeBucket][parent][child]) {
            hasTemporalLevel4Child[timeBucket][parent][child] = true;
            temporalLevel4Children[timeBucket][parent].push(child);
        }
    }

    function _linkTemporal56(uint256 timeBucket, bytes5 parent, bytes6 child) internal {
        if (!hasTemporalLevel5Child[timeBucket][parent][child]) {
            hasTemporalLevel5Child[timeBucket][parent][child] = true;
            temporalLevel5Children[timeBucket][parent].push(child);
        }
    }

    function _linkTemporal67(uint256 timeBucket, bytes6 parent, bytes7 child) internal {
        if (!hasTemporalLevel6Child[timeBucket][parent][child]) {
            hasTemporalLevel6Child[timeBucket][parent][child] = true;
            temporalLevel6Children[timeBucket][parent].push(child);
        }
    }

    function _validatedLeafGeohash(string memory geohash)
        internal pure returns (bytes7 leaf)
    {
        bytes memory g = bytes(geohash);
        require(g.length == LEAF_GEOHASH_DEPTH, "must be 7 chars");
        assembly { leaf := mload(add(g, 32)) }
    }
}
