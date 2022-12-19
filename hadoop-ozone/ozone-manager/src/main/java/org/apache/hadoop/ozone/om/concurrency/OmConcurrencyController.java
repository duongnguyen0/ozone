package org.apache.hadoop.ozone.om.concurrency;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.lock.OzoneManagerLock;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InfoBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeysArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeysMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Centre component works on OM leader to control the concurrency of all OM
 * operations. Basically, it determines which operations can happen concurrently
 * and which ones have to wait.
 *
 *            Read                   write log             write
 * Clients -----------> OM Leader ---------------> Ratis ---------> All OMs
 *           Write     (acquire lock)
 *
 */
public class OmConcurrencyController {
  public static final Logger LOG =
      LoggerFactory.getLogger(OmConcurrencyController.class);

  private final OMMetadataManager metadataManager;
  private final OzoneManagerLock omLock;

  public OmConcurrencyController(OMMetadataManager metadataManager) {
    this.metadataManager = metadataManager;
    this.omLock = metadataManager.getLock();
  }

  public LockHolder acquireLeaderLock(OMRequest request) throws IOException {
    switch (request.getCmdType()) {
      // Volume writes
    case CreateVolume:
      return VolumeLock.acquireWriteLock(omLock,
          request.getCreateVolumeRequest().getVolumeInfo().getVolume());
    case DeleteVolume:
      return VolumeLock.acquireWriteLock(omLock,
          request.getDeleteVolumeRequest().getVolumeName());
    case SetVolumeProperty:
      return VolumeLock.acquireWriteLock(omLock,
          request.getSetVolumePropertyRequest().getVolumeName());
    case CheckVolumeAccess:
      return VolumeLock.acquireReadLock(omLock,
          request.getCheckVolumeAccessRequest().getVolumeName());
      // Volume reads
    case InfoVolume:
      return VolumeLock.acquireReadLock(omLock,
          request.getInfoVolumeRequest().getVolumeName());

      // bucket writes
    case CreateBucket:
      BucketInfo bucketInfo = request.getCreateBucketRequest().getBucketInfo();
      return BucketLock.acquireWriteLock(omLock,
          bucketInfo.getVolumeName(), bucketInfo.getBucketName());
    case SetBucketProperty:
      BucketArgs bucketArgs =
          request.getSetBucketPropertyRequest().getBucketArgs();
      return BucketLock.acquireWriteLock(omLock,
          bucketArgs.getVolumeName(), bucketArgs.getBucketName());
    case DeleteBucket:
      DeleteBucketRequest delBucketRequest =
          request.getDeleteBucketRequest();
      return BucketLock.acquireWriteLock(omLock,
          delBucketRequest.getVolumeName(), delBucketRequest.getBucketName());

      // bucket reads
    case InfoBucket:
      InfoBucketRequest infoBucketRequest =
          request.getInfoBucketRequest();
      return BucketLock.acquireReadLock(omLock,
          infoBucketRequest.getVolumeName(), infoBucketRequest.getBucketName());

      // key(s) writes
    case CreateKey:
      return KeyLock.acquireWriteLock(metadataManager,
          request.getCreateKeyRequest().getKeyArgs());
    case DeleteKey:
      return KeyLock.acquireWriteLock(metadataManager,
          request.getDeleteKeyRequest().getKeyArgs());
    case CommitKey:
      return KeyLock.acquireWriteLock(metadataManager,
          request.getCommitKeyRequest().getKeyArgs());
    case AllocateBlock:
      return KeyLock.acquireWriteLock(metadataManager,
          request.getAllocateBlockRequest().getKeyArgs());
    case RenameKey:
      KeyArgs keyArgs = request.getRenameKeyRequest().getKeyArgs();
      return KeyLock.acquireWriteLock(metadataManager,
          keyArgs.getVolumeName(), keyArgs.getBucketName(),
          keyArgs.getKeyName(),
          request.getRenameKeyRequest().getToKeyName());

    // TODO: To simplify things, all the batch operations below should be
    //  transformed into multiple single requests by OM leader.
    case RenameKeys:
      RenameKeysArgs renameKeysArgs =
          request.getRenameKeysRequest().getRenameKeysArgs();
      String[] keys = new String[renameKeysArgs.getRenameKeysMapCount() * 2];
      int i = 0;
      for (RenameKeysMap renameKey : renameKeysArgs.getRenameKeysMapList()) {
        keys[i++] = renameKey.getFromKeyName();
        keys[i++] = renameKey.getToKeyName();
      }
      return KeyLock.acquireWriteLock(metadataManager, renameKeysArgs.getVolumeName(),
          renameKeysArgs.getBucketName(), keys);
    case DeleteKeys:
      DeleteKeyArgs
          deleteKeyArgs = request.getDeleteKeysRequest().getDeleteKeys();
      keys = deleteKeyArgs.getKeysList().toArray(new String[0]);
      return KeyLock.acquireWriteLock(metadataManager,
          deleteKeyArgs.getVolumeName(), deleteKeyArgs.getBucketName(), keys);
    case DeleteOpenKeys:
      // TODO: this include keys from different buckets,
      return LockHolder.EMPTY;
    case InitiateMultiPartUpload:
      return KeyLock.acquireWriteLock(metadataManager,
          request.getInitiateMultiPartUploadRequest().getKeyArgs());
    case CommitMultiPartUpload:
      return KeyLock.acquireWriteLock(metadataManager,
          request.getCommitMultiPartUploadRequest().getKeyArgs());
    case CompleteMultiPartUpload:
      return KeyLock.acquireWriteLock(metadataManager,
          request.getCompleteMultiPartUploadRequest().getKeyArgs());
    case AbortMultiPartUpload:
      return KeyLock.acquireWriteLock(metadataManager,
          request.getAbortMultiPartUploadRequest().getKeyArgs());

    case LookupFile:
      return KeyLock.acquireReadLock(metadataManager,
          request.getLookupFileRequest().getKeyArgs());
    case LookupKey:
      return KeyLock.acquireReadLock(metadataManager,
          request.getLookupKeyRequest().getKeyArgs());
    case GetKeyInfo:
      return KeyLock.acquireReadLock(metadataManager,
          request.getGetKeyInfoRequest().getKeyArgs());

    default:
      LOG.debug("No explicit concurrency handling for {}.",
          request.getCmdType());
      return LockHolder.EMPTY;
    }
  }
}
