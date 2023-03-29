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

#ifndef SRC_CLIENT_H_
#define SRC_CLIENT_H_

// Minimalist SciDB client API
struct QueryID
{
  unsigned long long coordinatorid;
  unsigned long long queryid;
};

#define SHIM_CONNECTION_SUCCESSFUL  0
#define SHIM_ERROR_CANT_CONNECT    -1
#define SHIM_ERROR_AUTHENTICATION  -2

void *c_scidb_connect(
    const char *host,
    int port,
    const char* username,
    const char* password,
    int isAdmin,
    int* status);

int c_scidb_disconnect (void *con);

void* c_init_query_result();

void c_free_query_result(void *queryresult);

struct QueryID c_query_result_to_id(void *queryresult);

#define SHIM_PREPARATION_SUCCESS    0
#define SHIM_NO_QUERY_RESULT_OBJ   -1
#define SHIM_PREPARATION_ERROR     -2

int c_prepare_query (void *con, const char *query, void *queryresult, char *err);

#define SHIM_EXECUTION_SUCCESS      0
#define SHIM_TRANSACTION_ROLLBACK  -3
#define SHIM_EXECUTION_ERROR       -4

int c_execute_prepared_query (void *con, const char *query, void *queryresult, char *err);

#define SHIM_COMPLETION_SUCCESS     0
#define SHIM_COMPLETION_INVALID    -5
#define SHIM_COMPLETION_ERROR      -6

int c_complete_query (void *con, void *queryresult, char *err);

#define SHIM_NO_SCIDB_CONNECTION   -7

#define SHIM_IO_ERROR              -8

#endif /* SRC_CLIENT_H_ */
