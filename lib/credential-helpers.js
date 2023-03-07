import { querySudo as query, updateSudo as update } from "@lblod/mu-auth-sudo";
import { sparqlEscapeUri } from "mu";
import { BASIC_AUTH, OAUTH2, PREFIXES } from "../constants";


/**
 * Gets CredentialsType from RemoteDataObject
 * @param {String} remoteDataObjectUri
 * @returns credentialsType
 */

async function getCredentialsTypeForRemoteDataObject(remoteDataObjectUri) {
  const credentialsTypeQuery = `
    PREFIX dgftSec: <http://lblod.data.gift/vocabularies/security/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>

    SELECT DISTINCT ?securityConfigurationType WHERE {
        ${sparqlEscapeUri(
          remoteDataObjectUri
        )} dgftSec:targetAuthenticationConfiguration ?authenticationConf .
        ?authenticationConf dgftSec:securityConfiguration/rdf:type ?securityConfigurationType .
        VALUES ?securityConfigurationType {
          <https://www.w3.org/2019/wot/security#BasicSecurityScheme>
          <https://www.w3.org/2019/wot/security#OAuth2SecurityScheme>
      }
    }
  `;
  const credentialsType = await query(credentialsTypeQuery);
  return credentialsType.results.bindings[0]
    ? credentialsType.results.bindings[0].securityConfigurationType.value
    : null;
}

/**
 * Deletes credentials from remoteDataObject.
 * @param {String} remoteDataObjectUri
 */

export async function deleteCredentials(remoteDataObjectUri) {
  let credentialsType;
  let cleanOauth2Query = `
      ${PREFIXES}
      DELETE {
        GRAPH ?g {
          ?configuration dgftSec:secrets ?secrets .
          ?secrets dgftOauth:clientId ?clientId ;
            dgftOauth:clientSecret ?clientSecret .
        }
      } WHERE {
      
        ${sparqlEscapeUri(
          remoteDataObjectUri
        )} dgftSec:targetAuthenticationConfiguration ?configuration .
      
        GRAPH ?g {
          ?configuration dgftSec:secrets ?secrets .
          ?secrets dgftOauth:clientId ?clientId ;
            dgftOauth:clientSecret ?clientSecret .
        }
      }
      `;
  let cleanBasicAuthQuery = `
      ${PREFIXES}
      DELETE {
        GRAPH ?g {
          ?configuration dgftSec:secrets ?secrets .
          ?secrets meb:username ?user ;
            muAccount:password ?pass .
        }
      } WHERE {
      
        ${sparqlEscapeUri(
          remoteDataObjectUri
        )} dgftSec:targetAuthenticationConfiguration ?configuration .
      
        GRAPH ?g {
          ?configuration dgftSec:secrets ?secrets .
          ?secrets meb:username ?user ;
            muAccount:password ?pass .
        }
      }
      `;

  if (!credentialsType)
    credentialsType = await getCredentialsTypeForRemoteDataObject(
      remoteDataObjectUri
    );

  switch (credentialsType) {
    case BASIC_AUTH:
      await update(cleanBasicAuthQuery);
      break;
    case OAUTH2:
      await update(cleanOauth2Query);
      break;
    default:
      return false;
  }
}
