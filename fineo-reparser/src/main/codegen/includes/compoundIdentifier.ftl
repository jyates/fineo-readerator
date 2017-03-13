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
SqlIdentifier build_fineo_compound_ident(DrillCompoundIdentifier.Builder builder, SqlIdentifier s, int start){
  for(int i=start; i < s.names.size(); i++){
    builder.addString(s.names.get(i), s.getComponentParserPosition(i));
  }
  return builder.build();
}

JAVACODE
SqlIdentifier build_fineo_user_component(DrillCompoundIdentifier.Builder builder, SqlIdentifier s, boolean hasFineo){
  builder.addString(this.org, s.getComponentParserPosition(0));
  return build_fineo_compound_ident(builder, s, hasFineo ? 1: 0);
}

/**
 * Parses a Drill compound identifier.
 * Uses a varargs to support the existing implementations and also prefixing with Fineo
 */
SqlIdentifier CompoundIdentifier(boolean ... flag) :
{
    DrillCompoundIdentifier.Builder builder = DrillCompoundIdentifier.newBuilder();
    String p;
    int index;
    boolean hasFineo;
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
      // information schema doesn't get modified
      if(standard.names.get(0).equals("INFORMATION_SCHEMA")){
        return standard;
      }

      // prepend the fineo parts, if we need to:
      // 1. Compound flag is enabled
      // 2. We aren't already prefixing with 'fineo'
      // 3. Prefixing with 'fineo', but not followed by the org id
      if(flag != null && flag.length > 0 && flag[0] == true) {
        hasFineo = standard.names.get(0).equals("fineo");
        builder = DrillCompoundIdentifier.newBuilder();
        builder.addString("fineo", standard.getComponentParserPosition(0));

        // error table just gets a fineo prepend. orgid is managed as where clauses after rewrite
        if(standard.names.get(0).equals("errors")){
          // asking about an error table, make it fineo.error.<table>
          // (by adding error.<table> onto the current builder)
          if(standard.names.size() == 2){
             return build_fineo_compound_ident(builder, standard, hasFineo? 1: 0);
          }
        }

        // its a regular table, prepend an org id
        if(standard.names.size() <= 2 || !standard.names.get(1).equals(this.org)){
          standard = build_fineo_user_component(builder, standard, hasFineo);
        }
      }
      return standard;
    }
}
