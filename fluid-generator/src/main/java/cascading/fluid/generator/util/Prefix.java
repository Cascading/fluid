/*
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.fluid.generator.util;

import java.nio.charset.Charset;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 *
 */
public class Prefix<P, Lhs, Rhs>
  {
  P prefix;
  Pair<Lhs, Rhs> pair;
  Map<String, Object> payload;

  public Prefix( P prefix )
    {
    this.prefix = prefix;
    }

  public Prefix( P prefix, Lhs lhs, Rhs rhs )
    {
    this( prefix, new Pair<Lhs, Rhs>( lhs, rhs ) );
    }

  public Prefix( P prefix, Pair<Lhs, Rhs> pair )
    {
    this.prefix = prefix;
    this.pair = pair;
    }

  public P getPrefix()
    {
    return prefix;
    }

  public Pair<Lhs, Rhs> getPair()
    {
    return pair;
    }

  public Lhs getLhs()
    {
    return pair.getLhs();
    }

  public Rhs getRhs()
    {
    return pair.getRhs();
    }

  public String getHash()
    {
    HashFunction hf = Hashing.md5();

    return hf.newHasher().putString( toString(), Charset.forName( "UTF-8" ) ).hash().toString();
    }

  public void addPayload( String key, Object value )
    {
    if( payload == null )
      payload = new LinkedHashMap<String, Object>();

    payload.put( key, value );
    }

  public Object getPayload( String key )
    {
    if( payload == null )
      return null;

    return payload.get( key );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;

    Prefix prefix1 = (Prefix) object;

    if( pair != null ? !pair.equals( prefix1.pair ) : prefix1.pair != null )
      return false;
    if( prefix != null ? !prefix.equals( prefix1.prefix ) : prefix1.prefix != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = prefix != null ? prefix.hashCode() : 0;
    result = 31 * result + ( pair != null ? pair.hashCode() : 0 );
    return result;
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder( "Prefix{" );
    sb.append( "prefix=" ).append( prefix );
    sb.append( ", pair=" ).append( pair );
    sb.append( '}' );
    return sb.toString();
    }

  public String print()
    {
    final StringBuilder sb = new StringBuilder( "" );
    if( pair == null )
      sb.append( prefix );
    else
      sb.append( pair.print() );
    return sb.toString();
    }
  }
