/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gora.persistency.impl;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import org.apache.avro.Schema.Field;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.gora.persistency.Dirtyable;
import org.apache.gora.persistency.Persistent;

/**
 * Base classs implementing common functionality for Persistent
 * classes.
 */
public abstract class PersistentBase extends SpecificRecordBase implements Persistent  {

  public static class PersistentData extends SpecificData {
    private static final PersistentData INSTANCE = new PersistentData();

    public static PersistentData get() {
      return INSTANCE;
    }

    public boolean equals(SpecificRecord obj1, SpecificRecord that) {
      if (that == obj1)
        return true; // identical object
      if (!(that instanceof SpecificRecord))
        return false; // not a record
      if (obj1.getClass() != that.getClass())
        return false; // not same schema
      return PersistentData.get().compare(obj1, that, obj1.getSchema(), true) == 0;
    }

  }

  @Override
  public void clearDirty() {
    ByteBuffer dirtyBytes = getDirtyBytes();
    assert (dirtyBytes.position() == 0);
    for (int i = 0; i < dirtyBytes.limit(); i++) {
      dirtyBytes.put(i, (byte) 0);
    }
    for (Field field : getSchema().getFields()) {
      clearDirynessIfFieldIsDirtyable(field.pos());
    }
  }

  private void clearDirynessIfFieldIsDirtyable(int fieldIndex) {
    if (fieldIndex == 0)
      return;
    Object value = get(fieldIndex);
    if (value instanceof Dirtyable) {
      ((Dirtyable) value).clearDirty();
    }
  }

  @Override
  public void clearDirty(int fieldIndex) {
    ByteBuffer dirtyBytes = getDirtyBytes();
    assert (dirtyBytes.position() == 0);
    int byteOffset = fieldIndex / 8;
    int bitOffset = fieldIndex % 8;
    byte currentByte = dirtyBytes.get(byteOffset);
    currentByte = (byte) ((~(1 << bitOffset)) & currentByte);
    dirtyBytes.put(byteOffset, currentByte);
    clearDirynessIfFieldIsDirtyable(fieldIndex);
  }

  @Override
  public void clearDirty(String field) {
    clearDirty(getSchema().getField(field).pos());
  }

  @Override
  public boolean isDirty() {
    List<Field> fields = getSchema().getFields();
    boolean isSubRecordDirty = false;
    for (Field field : fields) {
      isSubRecordDirty = isSubRecordDirty || checkIfMutableFieldAndDirty(field);
    }
    ByteBuffer dirtyBytes = getDirtyBytes();
    assert (dirtyBytes.position() == 0);
    boolean dirty = false;
    for (int i = 0; i < dirtyBytes.limit(); i++) {
      dirty = dirty || dirtyBytes.get(i) != 0;
    }
    return isSubRecordDirty || dirty;
  }

  private boolean checkIfMutableFieldAndDirty(Field field) {
    if (field.pos() == 0)
      return false;
    switch (field.schema().getType()) {
    case RECORD:
    case MAP:
    case ARRAY:
      return ((Dirtyable) get(field.pos())).isDirty();
    case UNION:
      Object value = get(field.pos());
      if (value instanceof Dirtyable) {
        return ((Dirtyable) value).isDirty();
      }
    default:
      // TODO add sufficient logging
      break;
    }
    return false;
  }

  @Override
  public boolean isDirty(int fieldIndex) {
    Field field = getSchema().getFields().get(fieldIndex);
    boolean isSubRecordDirty = checkIfMutableFieldAndDirty(field);
    ByteBuffer dirtyBytes = getDirtyBytes();
    assert (dirtyBytes.position() == 0);
    int byteOffset = fieldIndex / 8;
    int bitOffset = fieldIndex % 8;
    byte currentByte = dirtyBytes.get(byteOffset);
    return isSubRecordDirty || 0 != ((1 << bitOffset) & currentByte);
  }

  @Override
  public boolean isDirty(String fieldName) {
    Field field = getSchema().getField(fieldName);
    if(field == null){
      throw new IndexOutOfBoundsException("Field "+ fieldName + " does not exist in this schema.");
    }
    return isDirty(field.pos());
  }

  @Override
  public void setDirty() {
    ByteBuffer dirtyBytes = getDirtyBytes();
    assert (dirtyBytes.position() == 0);
    for (int i = 0; i < dirtyBytes.limit(); i++) {
      dirtyBytes.put(i, (byte) -128);
    }
  }

  @Override
  public void setDirty(int fieldIndex) {
    ByteBuffer dirtyBytes = getDirtyBytes();
    assert (dirtyBytes.position() == 0);
    int byteOffset = fieldIndex / 8;
    int bitOffset = fieldIndex % 8;
    byte currentByte = dirtyBytes.get(byteOffset);
    currentByte = (byte) ((1 << bitOffset) | currentByte);
    dirtyBytes.put(byteOffset, currentByte);
  }

  @Override
  public void setDirty(String field) {
    setDirty(getSchema().getField(field).pos());
  }

  private ByteBuffer getDirtyBytes() {
    return (ByteBuffer) get(0);
  }

  @Override
  public void clear() {
    Collection<Field> unmanagedFields = getUnmanagedFields();
    for (Field field : getSchema().getFields()) {
      if (!unmanagedFields.contains(field))
        continue;
      /*
* TODO: Its more in the spirit of Gora's clear method to actually clear
* data structures, but since avro no-longer defaults to having empty
* structures the way to do this consistently would be complicated.
*/
      put(field.pos(), null);
    }
    clearDirty();
  }

  @Override
  public boolean equals(Object that) {
    if (that == this) {
      return true;
    } else if (that instanceof Persistent) {
      return PersistentData.get().equals(this, (SpecificRecord) that);
    } else {
      return false;
    }
  }
  
  public List<Field> getUnmanagedFields(){
    List<Field> fields = getSchema().getFields();
    return fields.subList(1, fields.size());
  }
  
}
