<#-- Licensed to the Apache Software Foundation (ASF) under one or more contributor
  license agreements. See the NOTICE file distributed with this work for additional
  information regarding copyright ownership. The ASF licenses this file to
  You under the Apache License, Version 2.0 (the "License"); you may not use
  this file except in compliance with the License. You may obtain a copy of
  the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required
  by applicable law or agreed to in writing, software distributed under the
  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
  OF ANY KIND, either express or implied. See the License for the specific
  language governing permissions and limitations under the License.

  Fork of the Drill implementation to support specifying the table prefixed with "fineo" and the
  organization key.
  -->

JAVACODE
SqlIdentifier build_fineo_compound_ident(DrillCompoundIdentifier.Builder builder, SqlIdentifier s){
  for(int i=0; i < s.names.size(); i++){
    builder.addString(s.names.get(i), s.getComponentParserPosition(i));
  }
  return builder.build();
}

/**
 * Parses a Drill compound identifier.
 * Uses a varargs to support the existing implementations
 */
SqlIdentifier CompoundIdentifier(boolean ... flag) :
{
    DrillCompoundIdentifier.Builder builder = DrillCompoundIdentifier.newBuilder();
    String p;
    int index;
}
{
    p = Identifier()
    {
        builder.addString(p, getPos());
    }
    (
        (
        <DOT>
        (
            p = Identifier() {
                builder.addString(p, getPos());
            }
        |
            <STAR> {
                builder.addString("*", getPos());
            }
        )
        )
        |
        (
          <LBRACKET>
          index = UnsignedIntLiteral()
          <RBRACKET> 
          {
              builder.addIndex(index, getPos());
          }
        )
    ) *
    {
      SqlIdentifier standard = builder.build();
      // prepend the fineo parts, if we need to
      if(flag != null && flag.length > 0 && flag[0] == true) {
        if(standard.names.size() <= 2 ||
           !(standard.names.get(0).equals("fineo") && standard.names.get(1).equals(this.org))){
              builder = DrillCompoundIdentifier.newBuilder();
              builder.addString("fineo", standard.getComponentParserPosition(0));
              builder.addString(this.org, standard.getComponentParserPosition(0));
              standard = build_fineo_compound_ident(builder, standard);
          }
      }
      return standard;
    }
}
