/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2023 Paradigm4, Inc.
*
* shim is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation version 3 of the License.
*
* This software is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY, NON-INFRINGEMENT, OR
* FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for the
* complete license terms.
*
* You should have received a copy of the GNU General Public License
* along with shim.  If not, see <http://www.gnu.org/licenses/>.
*
* END_COPYRIGHT
*/

/* Minimal SciDB Client Interface */
#include <string>
#include <iostream>
#include <stdio.h>
#include <fstream>
#include <algorithm>

#define MAX_VARLEN 4096

extern "C"
{
#include "client.h"
}

#include "SciDBAPI.h"
#include "rbac/SessionProperties.h"
#include "rbac/Credential.h"

using namespace std;

/* Connect to a SciDB instance on the specified host and port.
 * Returns a pointer to the SciDB connection context, or NULL
 * if an error occurred.
 */
extern "C" void * c_scidb_connect(
    const char *host,
    int port,
    const char* username,
    const char* password,
    int isAdmin,
    int* status)
{
  void* conn = NULL;
  scidb::SciDBClient& db = scidb::getSciDB();
  try {
      scidb::SessionProperties props;

      // Set credentials, if any
      if (username == NULL || password == NULL) {
          props.setCredCallback(NULL, NULL); // SDB-6038
      } else {
          props.setCred(scidb::Credential(username, password));
      }

      // Set admin, if enabled
      if (isAdmin) {
          props.setPriority(scidb::SessionProperties::ADMIN);
      }

      // Attempt to connect
      conn = db.connect(props, host, port);
      *status = SHIM_CONNECTION_SUCCESSFUL;
  } catch(const scidb::Exception& se) {
      if (se.getLongErrorCode() == scidb::SCIDB_LE_AUTHENTICATION_ERROR) {
          *status = SHIM_ERROR_AUTHENTICATION;
      } else {
          *status = SHIM_ERROR_CANT_CONNECT;
      }
      conn = NULL;
  }
  catch(std::exception& e) {
      *status = SHIM_ERROR_CANT_CONNECT;
      conn = NULL;
  }
  return conn;
}

/* Disconnect a connected SciDB client connection,
 * the con pointer should be from the scidbconnect function above.
 * Returns 0 on success and 1 on failure
 */
extern "C" int c_scidb_disconnect(void *con)
{
  scidb::SciDBClient& db = scidb::getSciDB();
  try {
    db.disconnect((void *)con);
    return 0;
  } catch(std::exception& e) {
    return 1;
  }
}

extern "C" void* c_init_query_result()
{
  return static_cast<void*>(new scidb::QueryResult());
}

extern "C" void c_free_query_result(void *queryresult)
{
  auto q = static_cast<scidb::QueryResult*>(queryresult);
  delete q;
}

extern "C" QueryID c_query_result_to_id(void *queryresult) {
  auto q = static_cast<scidb::QueryResult*>(queryresult);
  QueryID qid {0, 0};
  qid.queryid = q->queryID.getId();
  qid.coordinatorid = q->queryID.getCoordinatorId();
  return qid;
}

/* Prepare a query using the indicated SciDB client connection.
 * result: Pointer to a ShimPreppedQuery, filled in by this function on success
 * con: a scidb connection context
 * err: a buff of length MAX_VARLEN to hold  error string should one occur
 *
 * Upon returning, either the results structure is populated with a non-NULL
 * queryresult pointer and a valid queryid and the err string is NULL, or
 * ther err string is not NULL and the queryresult is NULL.
 */
extern "C" int
c_prepare_query(void *con, const char *query, void *queryresult, char *err)
{
  // Prepare returned object
  if (!queryresult) {
    snprintf(err,MAX_VARLEN,"Invalid query result pointer\n");
    return SHIM_NO_QUERY_RESULT_OBJ;
  }

  // Prepare query
  scidb::SciDBClient& db = scidb::getSciDB();
  const string &queryString = query;
  auto q = static_cast<scidb::QueryResult*>(queryresult);
  try {
    db.prepareQuery(queryString, true, "", *q, con);
  } catch(std::exception& e) {
    snprintf(err,MAX_VARLEN,"%s",e.what());
    return SHIM_PREPARATION_ERROR;
  }

  return SHIM_PREPARATION_SUCCESS;
}

/* Execute a prepared requery stored in the ShimPreppedQuery pq on the scidb
 * connection context con.
 * The char buffer err is a buffer of length MAX_VARLEN on input that will hold an
 * error string on output, should one occur. The queryresult object pointed to
 * from within pq is de-allocated by this function.  Successful exit returns
 * a sQueryID struct.  Failure populates the err buffer with an error string and
 * returns an error code of 1
 */
extern "C" int
c_execute_prepared_query(void *con, const char *query, void *queryresult, char *err)
{
  // Examine prepped query
  if (!queryresult) {
    snprintf(err,MAX_VARLEN,"Invalid query result object.\n");
    return SHIM_NO_QUERY_RESULT_OBJ;
  }
  auto *qr = static_cast<scidb::QueryResult*>(queryresult);

  scidb::SciDBClient& db = scidb::getSciDB();
  const string &queryString = query;
  try {
    db.executeQuery(queryString, true, *qr, (void *)con);
  } catch (scidb::RollbackException const& rbe) {
    // SDB-7521: RollbackException indicates that a transaction was rolled back successfully
    // in response to a user's rollback() command. So the rollback() query was successful, and
    // shim should return the query ID to the client, like it does with a normal successful query.
    //
    // However, we can't call completeQuery() on the query, because it was rolled back.
    // So we return an error code to tell the caller not to run completeQuery().
    return SHIM_TRANSACTION_ROLLBACK;
  } catch (std::exception const& e) {
    snprintf(err,MAX_VARLEN,"%s",e.what());
    return SHIM_EXECUTION_ERROR;
  }
  return SHIM_EXECUTION_SUCCESS;
}

extern "C" int c_complete_query(void *con, void *queryresult, char *err)
{
  // Examine prepped/executed query
  if (!queryresult) {
    snprintf(err,MAX_VARLEN,"Invalid query result object.\n");
    return SHIM_NO_QUERY_RESULT_OBJ;
  }
  auto *qr = static_cast<scidb::QueryResult*>(queryresult);

  if (!qr->queryID.isValid()) {
    return SHIM_COMPLETION_INVALID;
  }
  
  if (qr->autoCommit) {
    return SHIM_COMPLETION_SUCCESS; // not an error
  } else {
  scidb::SciDBClient& db = scidb::getSciDB();
    try {
      db.completeQuery(qr->queryID, (void *)con);
    } catch(std::exception& e) {
      snprintf(err,MAX_VARLEN,"%s",e.what());
      return SHIM_COMPLETION_ERROR;
    }
  }

  return SHIM_COMPLETION_SUCCESS;
}
