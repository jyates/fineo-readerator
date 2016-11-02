# Parser Generation

We create a custom parser to enforce that each server can only serve a single org's data. This is
 a light fork on the existing Drill/Calcite parser. The parser should be generated during the 
 "generate-sources" phase of the build
  
## Implementation

We add a couple of features to the parser (see below 'Updating') that enable us to replace how we
 handle table identifiers. Whenever we find a table identifier request, we ask for a 'flagged' 
 CompoundIdentifier which adds the ("fineo", org) prefix to the CompoundIdentifier's names.
 
The org is specified in the settings for the parser and are currently only set once, by our 
custom parser factory.

## Updating

Unfortunately, Drill doesn't make their calcite fork easily available. Thus, you need to run:
```mvn clean initialize -Pextract-drill-parser``` in the Drill repo to extract the existing parser 
(Parser.jj) into ```target```. From there, you need to move it to ```main/codegen/templates``` and
apply our custom features on top of it. These features include:

  * Supporting custom settings in parser via flag + include/.ftl
  * Supporting special handling for tableIdentifiers
  * Supporting replacing default CompoundIdentifier

Eventually, these should go back upstream into drill/calcite. 

We also include the extra drill parser impls/keywords (things like show schema) so we can be as 
seamless as possible with the existing parsing. We explicitly don't include:

  * CreateTable
  * DropTable
  * CreateOrReplaceView
  * UseSchema
  * DropView
  * ShowFiles
  * RefreshMetadata

And we don't includen command/key words:

  * USE
  * FILES
  * DATABASES
  * SCHEMAS
  
As they prevent us from hiding information from the customers about other customers and 
potentially accessing data they shouldn't be able to access.
