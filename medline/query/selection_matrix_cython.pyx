# encoding: utf-8


from cpython cimport array
import array
from scipy.sparse import csr_matrix
from cython cimport boundscheck, wraparound



import numpy as np
cimport numpy as np


DTYPE_FLOAT64 = np.float64
ctypedef np.float64_t DTYPE_FLOAT64_t
DTYPE_INT = np.int32
ctypedef np.int_t DTYPE_INT_t



@boundscheck(False)
@wraparound(False)
def create_selection_matrix(np.ndarray[np.float64_t, ndim=1] m_data,
                            np.ndarray[np.int32_t, ndim=1] m_indices,
                            np.ndarray[np.int32_t, ndim=1] m_indptr,
                            matrix_shape,
                            np.ndarray[np.int32_t, ndim=1] rowids):

    cdef int start, end, i, j, rowid, rowids_length = len(rowids)

    # Initialize selection matrix arrays, add first line to indptr
    s_data = array.array(str('d'))
    s_indices = array.array(str('i'))
    s_indptr = array.array(str('i'))
    s_indptr.append(0)

    for i in range(rowids_length):
        rowid = rowids[i]

        # Start and end points of row are the values stored by indptr for rowid and (rowid + 1) - 1
        start = m_indptr[rowid]
        end = m_indptr[rowid+1]

        # Extend data and indices arrays with the data and indices between start and end
        s_data.extend(m_data[start:end])
        s_indices.extend(m_indices[start:end])

        # Add start of next row to indptr
        s_indptr.append(len(s_indices))

    # Store arrays in buffers (copied from sklearn)
    s_data = np.frombuffer(s_data, dtype=np.float64)
    s_indices = np.frombuffer(s_indices, dtype=np.intc)
    s_indptr = np.frombuffer(s_indptr, dtype=np.intc)

    s_matrix = csr_matrix( (s_data, s_indices, s_indptr), shape=(len(rowids), matrix_shape[1]), dtype=np.float64)

    return s_matrix

def make_int_array(type='i'):
    """
    Construct an array.array of a type suitable for scipy.sparse indices.
    Copied from scikit learn
    See https://docs.python.org/2/library/array.html
    imo, 'i' -> 4byte int, 'l' -> 8 byte int, 'd' -> 8 byte float
    """



if __name__ == "__main__":

    pass
